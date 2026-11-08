#!/usr/bin/env python3
"""Black-box policy regression checks; all Git mutations stay in temp fixtures."""

import json
import os
from pathlib import Path
import shutil
import subprocess
import sys
import tempfile
import unittest


POLICY = Path(__file__).resolve().with_name("commit-policy.py")
INSTALLER = Path(__file__).resolve().with_name("install-git-hooks.sh")
COMMIT_HOOK = Path(os.environ.get("COMMIT_POLICY_HOOK", str(
    POLICY.parent.parent / ".githooks/commit-msg")))
PRE_PUSH_HOOK = COMMIT_HOOK.with_name("pre-push")
BODY = "改动摘要：修复边界\n\n影响面：提交检查\n\n验证证据：反例通过\n"
ZERO = "0" * 40


class PolicyTests(unittest.TestCase):
    def setUp(self):
        self.temp = tempfile.TemporaryDirectory(prefix="commit-policy-test-")
        self.addCleanup(self.temp.cleanup)
        self.repo = Path(self.temp.name)
        self.env = {k: v for k, v in os.environ.items() if not k.startswith("GIT_")}
        self.env.update(GIT_AUTHOR_NAME="Policy fixture",
                        GIT_AUTHOR_EMAIL="fixture@example.invalid",
                        GIT_COMMITTER_NAME="Policy fixture",
                        GIT_COMMITTER_EMAIL="fixture@example.invalid",
                        GIT_CONFIG_NOSYSTEM="1", GIT_CONFIG_GLOBAL=os.devnull)
        self.git("init", "-q", "-b", "main")
        self.counter = 0
        self.baseline = self.commit("old nonconforming title", body="")

    def git(self, *args, env=None, **kwargs):
        return subprocess.check_output(
            ["git", "-c", "core.hooksPath=/dev/null", "-c", "commit.gpgSign=false", *args],
            cwd=self.repo, env=env or self.env, text=True,
            encoding="utf-8", stderr=subprocess.STDOUT, **kwargs).strip()

    def commit(self, subject="fix: 修复边界", body=BODY):
        self.counter += 1
        path = "fixture-%d.txt" % self.counter
        (self.repo / path).write_text("content\n", encoding="utf-8")
        message = self.repo / "message.txt"
        message.write_text(subject + "\n\n" + body, encoding="utf-8")
        self.git("add", "--", path)
        self.git("commit", "-q", "-F", str(message), "--", path)
        return self.git("rev-parse", "HEAD")

    def run_policy(self, *args, input_text=None, env=None):
        return subprocess.run(
            [sys.executable, str(POLICY), *args], cwd=self.repo,
            env=env or self.env, input=input_text, text=True, encoding="utf-8",
            capture_output=True)

    def message(self, subject, body=BODY, second=""):
        path = self.repo / "proposed-message.txt"
        path.write_text(subject + "\n" + second + "\n" + body, encoding="utf-8")
        return self.run_policy("message", str(path))

    def window(self, before=None, after=None, strict=False, **kwargs):
        args = ["range", "--before=" + (before or self.baseline),
                "--after=" + (after or self.git("rev-parse", "HEAD"))]
        args += ["--strict"] if strict else ["--baseline", self.baseline]
        return self.run_policy(*args, **kwargs)

    def assert_result(self, result, code=0, contains=None):
        output = result.stdout + result.stderr
        self.assertEqual(result.returncode, code, output)
        if contains:
            self.assertIn(contains, output)

    def test_chinese_summary_50_not_subject_length(self):
        self.assert_result(self.message("fix(long_scope): " + "中" * 50))

    def test_chinese_summary_51_fails(self):
        self.assert_result(self.message("fix: " + "中" * 51), 1, "[2]")

    def test_scope_optional(self):
        self.assert_result(self.message("fix: 修复边界"))
        self.assert_result(self.message("fix(auth-2_a): 修复边界"))

    def test_multi_scope_is_not_a_valid_new_subject(self):
        self.assert_result(self.message("fix(auth,ui): 修复边界"), 1, "[1]")

    def test_empty_summary(self):
        self.assert_result(self.message("fix:   "), 1, "[1]")

    def test_period(self):
        for period in (".", "。"):
            self.assert_result(self.message("fix: 修复边界" + period), 1, "[3]")

    def test_missing_blank_line(self):
        self.assert_result(self.message("fix: 修复边界", second="body"), 1, "[4]")

    def test_empty_and_whitespace_body(self):
        for body in ("", " \n\t\n"):
            self.assert_result(self.message("fix: 修复边界", body), 1, "[5]")

    def test_editor_comments_do_not_fill_body(self):
        self.assert_result(self.message("fix: 修复边界", "# edited file list\n"), 1, "[5]")
        self.git("config", "core.commentChar", ";")
        self.assert_result(self.message("fix: 修复边界", "; edited file list\n"), 1, "[5]")

    def test_raw_stdin_and_file_have_matching_core_rules(self):
        cases = [("fix(scope): " + "中" * 50, BODY, 0),
                 ("fix: " + "中" * 51, BODY, 1),
                 ("fix:   ", BODY, 1),
                 ("fix: 修复边界", " \n\t\n", 1)]
        for subject, body, expected in cases:
            text = subject + "\n\n" + body
            result = self.run_policy("message", "-", "--raw", input_text=text)
            self.assert_result(result, expected)
            path = self.repo / "stored.txt"
            path.write_text(text, encoding="utf-8")
            other = self.run_policy("message", str(path), "--raw")
            self.assertEqual((other.returncode, other.stdout),
                             (result.returncode, result.stdout))

    def test_normal_stdin_strips_comments_but_raw_preserves_history(self):
        text = "fix: 修复边界\n\n# Stored literal Markdown heading\n"
        self.assert_result(self.run_policy("message", "-", input_text=text), 1, "[5]")
        self.assert_result(self.run_policy("message", "-", "--raw", input_text=text))

    def test_optional_comment_cleanup_cannot_replace_subject(self):
        text = "# invalid literal subject\nfix: later valid-looking line\n\nbody\n"
        self.assert_result(self.run_policy("message", "-", input_text=text), 1, "[1]")

    def real_pathspec_commit(self, text):
        """Run the actual owned hook, including Git's temporary path index."""
        hooks = self.repo / ".githooks"
        hooks.mkdir()
        shutil.copy2(COMMIT_HOOK, hooks / "commit-msg")
        scripts = self.repo / "scripts"
        scripts.mkdir()
        shutil.copy2(POLICY, scripts / "commit-policy.py")
        (scripts / "commit-policy-baseline").write_text(self.baseline + "\n")
        for name in ("other.txt", "owned.txt"):
            (self.repo / name).write_text(name + "\n")
            self.git("add", "--", name)
        before = self.git("diff", "--cached", "--binary")
        message = self.repo / "literal-message.txt"
        message.write_text(text, encoding="utf-8")
        result = subprocess.run(
            ["git", "-c", "core.hooksPath=.githooks", "-c", "commit.gpgSign=false",
             "commit", "-q", "-F", str(message), "--", "owned.txt"],
            cwd=self.repo, env=self.env, text=True, encoding="utf-8", capture_output=True)
        return result, before

    def test_real_hook_F_rejects_original_comment_subject(self):
        # With -F Git preserves the leading # line. Stripping it in the hook
        # used to accept this message, then pre-push rejected the stored commit.
        result, before = self.real_pathspec_commit(
            "# invalid literal subject\nfix: validated replacement\n\nbody\n")
        self.assert_result(result, 1, "[1]")
        self.assertEqual(self.git("rev-parse", "HEAD"), self.baseline)
        self.assertEqual(self.git("diff", "--cached", "--binary"), before)

    def test_real_hook_F_preserves_literal_markdown_body(self):
        # This was previously accepted by stored-message checks but falsely
        # rejected at commit-msg because the literal heading was stripped.
        text = "docs: 合法Markdown正文\n\n# 正文字面标题\n"
        result, _ = self.real_pathspec_commit(text)
        self.assert_result(result)
        self.assertEqual(self.git("show", "-s", "--format=%B", "HEAD"), text.strip())
        self.assertEqual(self.git("diff", "--cached", "--name-only"), "other.txt")

    def check_real_hook_adoption_boundary(self, location, adopted, expected):
        """Legacy permission applies only to a genuinely external donor."""
        if location == "external":
            donor = tempfile.TemporaryDirectory(prefix="external-hook-donor-")
            self.addCleanup(donor.cleanup)
            hook_dir = Path(donor.name) / ".githooks"
        elif location == "nested":
            hook_dir = self.repo / ".legacy/.githooks"
        else:
            hook_dir = self.repo / ".githooks"
        hook_dir.mkdir(parents=True)
        for source in (COMMIT_HOOK, PRE_PUSH_HOOK):
            shutil.copy2(source, hook_dir / source.name)
        if location != "standard":
            donor_scripts = hook_dir.parent / "scripts"
            donor_scripts.mkdir()
            shutil.copy2(POLICY, donor_scripts / "commit-policy.py")
        if adopted:
            scripts = self.repo / "scripts"
            scripts.mkdir()
            (scripts / "commit-policy-baseline").write_text(self.baseline + "\n")
        # A content change keeps the historical amend guard from obscuring
        # the new policy boundary. No root-local policy implementation exists.
        (self.repo / "owned.txt").write_text("owned content\n")
        self.git("add", "--", "owned.txt")
        message = self.repo / "invalid-message.txt"
        message.write_text("invalid conventional subject\n")
        push_input = "refs/heads/main %s refs/heads/main %s\n" % (self.baseline, ZERO)
        for name, args, stdin in (
                ("commit-msg", [str(message)], None),
                ("pre-push", ["origin", "unused"], push_input)):
            with self.subTest(hook=name, location=location, adopted=adopted):
                result = subprocess.run([str(hook_dir / name), *args],
                    cwd=self.repo, env=self.env, input=stdin,
                    text=True, encoding="utf-8", capture_output=True)
                self.assert_result(result, expected,
                    "[WARN] inherited external hooks" if expected == 0 else
                    "[FAIL] local commit-policy is missing")

    def test_real_hooks_allow_unadopted_external_legacy(self):
        self.check_real_hook_adoption_boundary("external", False, 0)

    def test_real_hooks_fail_closed_for_missing_own_core(self):
        self.check_real_hook_adoption_boundary("standard", False, 1)

    def test_real_hooks_fail_closed_for_adopted_external_missing_core(self):
        self.check_real_hook_adoption_boundary("external", True, 1)

    def test_real_hooks_reject_nested_own_donor_as_external(self):
        self.check_real_hook_adoption_boundary("nested", False, 1)

    def test_baseline_grandfathers_old_history(self):
        self.commit()
        self.assert_result(self.window(before=ZERO), contains="1 commits checked")

    def test_strict_historical_range_detects_old_history(self):
        self.assert_result(self.window(before=ZERO, strict=True), 1, "[1]")

    def test_new_bad_message_is_not_grandfathered(self):
        self.commit("new invalid title")
        self.assert_result(self.window(), 1, "[1]")

    def test_empty_window(self):
        self.assert_result(self.window(), contains="0 commits checked")

    def test_missing_object_fails(self):
        self.assert_result(self.window(before="a" * 40), 2, "missing/non-commit")

    def test_non_commit_object_fails(self):
        tree = self.git("rev-parse", "HEAD^{tree}")
        self.assert_result(self.window(before=tree), 2, "missing/non-commit")

    def test_full_sha_required_and_no_shell_execution(self):
        for value in ("HEAD", "-all", "$(touch hacked)", "a" * 39, "z" * 40):
            self.assert_result(self.window(before=value), 2, "hexadecimal")
        self.assertFalse((self.repo / "hacked").exists())

    def test_missing_baseline_fails(self):
        self.assert_result(self.run_policy("range", "--before", ZERO, "--after",
                           self.baseline, "--baseline", "a" * 40), 2, "missing/non-commit")

    def test_initial_push_does_not_guess_20_commits(self):
        new = self.commit()
        line = "refs/heads/main %s refs/heads/new %s\n" % (new, ZERO)
        self.assert_result(self.run_policy("pre-push", "origin", "unused",
                           "--baseline", self.baseline, input_text=line),
                           contains="1 commits checked")

    def test_force_push_checks_replacement(self):
        old = self.commit()
        self.git("checkout", "-q", "-b", "replacement", self.baseline)
        new = self.commit("replacement invalid")
        self.assert_result(self.window(before=old, after=new), 1, "[1]")

    def test_delete_push_skips_valid_deleted_ref(self):
        line = "(delete) %s refs/heads/main %s\n" % (ZERO, self.baseline)
        self.assert_result(self.run_policy("pre-push", "--baseline", self.baseline,
                           input_text=line), contains="0 commits checked")

    def test_multiple_push_refs_are_all_checked(self):
        good = self.commit()
        bad = self.commit("invalid second ref")
        lines = "refs/heads/a %s refs/heads/a %s\nrefs/heads/b %s refs/heads/b %s\n" % (
            good, self.baseline, bad, self.baseline)
        self.assert_result(self.run_policy("pre-push", "--baseline", self.baseline,
                           input_text=lines), 1, "1/2")

    def test_malformed_push_input_fails(self):
        self.assert_result(self.run_policy("pre-push", "--baseline", self.baseline,
                           input_text="three fields only\n"), 2, "four fields")

    def test_default_pending_without_origin_is_initial_window(self):
        self.commit()
        self.assert_result(self.run_policy("pending", "--baseline", self.baseline),
                           contains="1 commits checked")

    def test_ci_uses_payload_head_not_checkout_head(self):
        candidate = self.commit()
        self.commit("checkout synthetic invalid")
        event = self.repo / "event.json"
        event.write_text(json.dumps({"pull_request": {
            "base": {"sha": self.baseline}, "head": {"sha": candidate}}}))
        self.assert_result(self.run_policy("ci", "--baseline", self.baseline,
                           "--event-name", "pull_request", "--event-file", str(event)),
                           contains="1 commits checked")

    def test_ci_missing_before_fails_not_empty(self):
        event = self.repo / "event.json"
        event.write_text(json.dumps({"before": "a" * 40, "after": self.baseline}))
        self.assert_result(self.run_policy("ci", "--baseline", self.baseline,
                           "--event-name", "push", "--event-file", str(event)),
                           2, "missing/non-commit")

    def test_all_new_merge_messages_are_checked(self):
        self.git("checkout", "-q", "-b", "side")
        self.commit()
        self.git("checkout", "-q", "main")
        self.commit()
        self.git("merge", "--no-ff", "side", "-m", "Merge branch 'side'")
        self.assert_result(self.window(), 1, "[1]")

    def test_conventional_merge_still_checks_bad_ancestor(self):
        self.git("checkout", "-q", "-b", "side")
        self.commit("bad side commit")
        self.git("checkout", "-q", "main")
        self.commit()
        self.git("merge", "--no-ff", "side", "-m", "chore: 合并审计修复\n\n" + BODY)
        self.assert_result(self.window(), 1, "1/3")

    def test_conventional_merge_with_good_ancestors_passes(self):
        self.git("checkout", "-q", "-b", "side")
        self.commit()
        self.git("checkout", "-q", "main")
        self.commit()
        self.git("merge", "--no-ff", "side", "-m", "chore: 合并审计修复\n\n" + BODY)
        self.assert_result(self.window(), contains="3 commits checked")

    def test_ci_can_fetch_exact_missing_force_push_before(self):
        old = self.commit()
        self.git("update-ref", "refs/keep/old", old)
        self.git("checkout", "-q", "-b", "replacement", self.baseline)
        new = self.commit()
        clone = self.repo / "clone"
        self.git("clone", "-q", "--no-local", "--single-branch", "-b", "replacement",
                 str(self.repo), str(clone))
        missing = subprocess.run(["git", "cat-file", "-e", old], cwd=clone,
                                 env=self.env, capture_output=True)
        self.assertNotEqual(missing.returncode, 0, "fixture must lack old force-push OID")
        event = self.repo / "event.json"
        event.write_text(json.dumps({"before": old, "after": new}))
        result = subprocess.run(
            [sys.executable, str(POLICY), "ci", "--baseline", self.baseline,
             "--event-name", "push", "--event-file", str(event), "--fetch-missing"],
            cwd=clone, env=self.env, text=True, capture_output=True)
        self.assert_result(result, contains="1 commits checked")

    def test_shallow_history_fails_closed(self):
        new = self.commit()
        clone = self.repo / "shallow"
        self.git("clone", "-q", "--depth", "1", self.repo.as_uri(), str(clone))
        result = subprocess.run(
            [sys.executable, str(POLICY), "range", "--before", ZERO,
             "--after", new, "--baseline", new], cwd=clone, env=self.env,
            text=True, capture_output=True)
        self.assert_result(result, 2, "shallow history")

    def test_worktree_install_does_not_disable_other_worktree_hooks(self):
        # Use subprocess directly: the fixture git helper overrides hooksPath.
        def real_git(*args, cwd=None):
            return subprocess.check_output(["git", *args], cwd=cwd or self.repo,
                env=self.env, text=True, stderr=subprocess.STDOUT).strip()
        legacy = str(self.repo / "legacy-hooks")
        real_git("config", "core.hooksPath", legacy)
        audit = self.repo / "audit"
        self.git("worktree", "add", "-q", "-b", "audit", str(audit))
        hooks = audit / ".githooks"
        hooks.mkdir()
        for name in ("pre-commit", "commit-msg", "pre-push"):
            hook = hooks / name
            hook.write_text("#!/bin/sh\nexit 0\n")
            hook.chmod(0o755)
        result = subprocess.run(["sh", str(INSTALLER)], cwd=audit,
            env=self.env, text=True, capture_output=True)
        self.assert_result(result)
        self.assertEqual(real_git("config", "--get", "core.hooksPath"), legacy)
        self.assertEqual(real_git("config", "--local", "--get", "core.hooksPath"), legacy)
        self.assertEqual(real_git("config", "--get", "core.hooksPath", cwd=audit), ".githooks")

    def test_installer_refuses_missing_hooks_before_config_change(self):
        result = subprocess.run(["sh", str(INSTALLER)], cwd=self.repo,
            env=self.env, text=True, capture_output=True)
        self.assert_result(result, 1, "missing executable hook")
        result = subprocess.run(["git", "config", "--get", "extensions.worktreeConfig"],
            cwd=self.repo, env=self.env, text=True, capture_output=True)
        self.assertEqual(result.returncode, 1)

    def test_index_override_and_paths_are_not_modified(self):
        (self.repo / "other.txt").write_text("another session\n")
        self.git("add", "--", "other.txt")
        index = self.repo / "private.index"
        env = dict(self.env, GIT_INDEX_FILE=str(index))
        self.git("read-tree", "HEAD", env=env)
        (self.repo / "owned.txt").write_text("owned\n")
        self.git("add", "--", "owned.txt", env=env)
        before_index = index.read_bytes()
        main_index = (self.repo / ".git/index").read_bytes()
        message = self.repo / "proposed.txt"
        message.write_text("fix: 修复边界\n\n" + BODY, encoding="utf-8")
        self.assert_result(self.run_policy("message", str(message), env=env))
        self.assertEqual(index.read_bytes(), before_index)
        self.assertEqual((self.repo / ".git/index").read_bytes(), main_index)
        self.assertEqual(self.git("diff", "--cached", "--name-only"), "other.txt")


if __name__ == "__main__":
    unittest.main(verbosity=2)
