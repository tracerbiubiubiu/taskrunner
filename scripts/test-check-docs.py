#!/usr/bin/env python3
"""Independent black-box document checker cases; only temporary Git repositories."""
import json
import os
from pathlib import Path
import subprocess
import sys
import tempfile
import unittest

CHECKER = Path(__file__).with_name('check-docs.py').resolve()


class DocumentChecks(unittest.TestCase):
    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory(prefix='zz-doc-check-')
        self.addCleanup(self.tmp.cleanup)
        self.repo = Path(self.tmp.name)
        self.env = {key: value for key, value in os.environ.items() if not key.startswith('GIT_')}
        self.env.update(GIT_CONFIG_NOSYSTEM='1', GIT_CONFIG_GLOBAL=os.devnull)
        self.git('init', '-q')
        self.git('config', 'user.name', 'Document Check Fixture')
        self.git('config', 'user.email', 'fixture@example.invalid')
        self.write('README.md', '# Start\n[existing](target.md#first)\n')
        self.write('target.md', '# First\n# Repeat\n# Repeat\n# 中文 标题\n')
        self.write('old-broken.md', '[historic](never-existed.md)\n')
        self.base = self.commit()

    def git(self, *args):
        return subprocess.check_output(['git', '-c', 'core.hooksPath=/dev/null', *args],
                                       cwd=self.repo, env=self.env, text=True).strip()

    def write(self, name, text):
        path = self.repo / name
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_text(text, encoding='utf-8')

    def commit(self):
        self.git('add', '.')
        self.git('commit', '-qm', 'fixture snapshot')
        return self.git('rev-parse', 'HEAD')

    def run_check(self, changes=(), **overrides):
        plan = dict(base=self.base, head=self.git('rev-parse', 'HEAD'), worktree=True,
                    mode='ordinary', changes=[{'status': 'M', 'path': p} if isinstance(p, str) else p
                                              for p in changes])
        plan.update(overrides)
        env = self.env.copy()
        env['DOC_TEST_PLAN'] = json.dumps(plan)
        return subprocess.run([sys.executable, str(CHECKER), '--plan-env', 'DOC_TEST_PLAN'],
                              cwd=self.repo, env=env, text=True, capture_output=True)

    def assert_ok(self, result):
        self.assertEqual(result.returncode, 0, result.stdout + result.stderr)

    def assert_bad(self, result, needle):
        self.assertNotEqual(result.returncode, 0, result.stdout + result.stderr)
        self.assertIn(needle.lower(), (result.stdout + result.stderr).lower())

    def test_missing_plan_is_rejected(self):
        env = self.env.copy()
        env.pop('DOC_TEST_NO_PLAN', None)
        result = subprocess.run([sys.executable, str(CHECKER), '--plan-env', 'DOC_TEST_NO_PLAN'],
                                cwd=self.repo, env=env, text=True, capture_output=True)
        self.assert_bad(result, 'plan')

    def test_head_must_be_current_candidate(self):
        self.write('other.txt', 'candidate\n')
        self.commit()
        self.assert_bad(self.run_check(head=self.base), 'head')

    def test_plan_paths_cannot_escape(self):
        for path in ['../outside.md', '/tmp/outside.md', 'docs/../../outside.json']:
            with self.subTest(path=path):
                self.assert_bad(self.run_check([path]), 'path')

    def test_missing_file_and_bad_fragment_fail(self):
        self.write('new.md', '[missing](missing.md)\n[bad](target.md#absent)\n')
        self.assert_bad(self.run_check(['new.md']), 'missing.md')
        self.write('new.md', '[bad](target.md#absent)\n')
        self.assert_bad(self.run_check(['new.md']), 'absent')

    def test_gfm_repeated_unicode_and_formatted_headings(self):
        self.write('target.md', '# First\n# Repeat\n# Repeat\n# 中文 标题\n# **Code** `Name` & More\n')
        self.write('new.md', '[first](target.md#repeat)\n[second](target.md#repeat-1)\n'
                   '[unicode](target.md#%E4%B8%AD%E6%96%87-%E6%A0%87%E9%A2%98)\n'
                   '[format](target.md#code-name--more)\n')
        self.assert_ok(self.run_check(['new.md', 'target.md']))

    def test_setext_and_explicit_html_anchors(self):
        self.write('target.md', 'Setext Title\n============\n<a id="named"></a>\n')
        self.write('new.md', '[setext](target.md#setext-title)\n[html](target.md#named)\n')
        # Change README too so its old #first link does not mask this case.
        self.write('README.md', '# Start\n')
        self.assert_ok(self.run_check(['new.md', 'target.md', 'README.md']))

    def test_horizontal_rule_does_not_duplicate_an_atx_heading(self):
        self.write('new.md', '# Title\n---\n[bad](#-title)\n')
        self.assert_bad(self.run_check(['new.md']), '-title')

    def test_reference_link_heading_uses_visible_text(self):
        self.write('new.md', '# [Visible][ref]\n[ref]: target.md\n[self](#visible)\n')
        self.assert_ok(self.run_check(['new.md']))

    def test_non_deleted_missing_change_is_rejected(self):
        self.assert_bad(self.run_check(['missing.md']), 'missing.md')

    def test_code_examples_do_not_create_links_or_headings(self):
        self.write('new.md', '# Actual\n```md\n# Fake\n[example](missing.md)\n```\n'
                   '`[example](also-missing.md)`\n[self](#actual)\n')
        self.assert_ok(self.run_check(['new.md']))
        self.write('new.md', self.repo.joinpath('new.md').read_text() + '[bad](#fake)\n')
        self.assert_bad(self.run_check(['new.md']), 'fake')

    def test_reference_and_html_links_are_checked(self):
        self.write('new.md', '[reference][target]\n\n[target]: <target.md#repeat-1> "title"\n'
                   '<a href="target.md#first">HTML</a>\n')
        self.assert_ok(self.run_check(['new.md']))
        self.write('new.md', '[reference][target]\n\n[target]: missing.md\n')
        self.assert_bad(self.run_check(['new.md']), 'missing.md')

    def test_balanced_parentheses_and_spaces_in_link_paths(self):
        self.write('a (copy).md', '# Heading\n')
        self.write('new.md', '[angle](<a (copy).md#heading>)\n[encoded](a%20%28copy%29.md#heading)\n')
        self.assert_ok(self.run_check(['new.md', 'a (copy).md']))

    def test_json_syntax_duplicate_keys_and_nonfinite_numbers(self):
        for text, needle in [('{"a":', 'json'), ('{"a":1,"a":2}', 'duplicate'),
                             ('{"nested":{"a":1,"a":2}}', 'duplicate'), ('{"a":NaN}', 'json')]:
            with self.subTest(text=text):
                self.write('candidate.json', text)
                self.assert_bad(self.run_check(['candidate.json']), needle)
        self.write('candidate.json', '{"items": [1, 2], "valid": true}')
        self.assert_ok(self.run_check(['candidate.json']))

    def test_deleted_target_checks_unchanged_inbound_but_not_old_debt(self):
        self.repo.joinpath('target.md').unlink()
        result = self.run_check([{'status': 'D', 'path': 'target.md'}])
        self.assert_bad(result, 'README.md')
        self.assertNotIn('never-existed.md', result.stdout + result.stderr)

    def test_removed_anchor_checks_unchanged_inbound(self):
        self.write('target.md', '# Replacement\n')
        self.assert_bad(self.run_check(['target.md']), 'README.md')

    def test_rename_checks_original_target_inbound(self):
        self.repo.joinpath('target.md').rename(self.repo / 'renamed.md')
        self.assert_bad(self.run_check([{'status': 'R100', 'old_path': 'target.md',
                                       'path': 'renamed.md'}]), 'README.md')

    def test_unchanged_historic_broken_links_do_not_block(self):
        self.write('target.md', self.repo.joinpath('target.md').read_text() + '\nText.\n')
        self.assert_ok(self.run_check(['target.md']))

    def test_unchanged_historic_path_escape_is_not_a_newly_broken_link(self):
        self.write('old-broken.md', '[historic private evidence](../../../../private/evidence.json)\n')
        self.base = self.commit()
        self.write('target.md', self.repo.joinpath('target.md').read_text() + '\nText.\n')
        self.assert_ok(self.run_check(['target.md']))

    def test_cross_repository_links_are_explicitly_unverified(self):
        self.write('new.md', '[sibling](../zhuzhao/docs/standards.md)\n'
                   '[remote](https://example.invalid/no-network)\n')
        result = self.run_check(['new.md'])
        self.assert_ok(result)
        self.assertIn('UNVERIFIED', result.stdout)
        self.assertIn('zhuzhao', result.stdout)
        self.write('new.md', '[secret](../unknown-repo/private.md)\n')
        self.assert_bad(self.run_check(['new.md']), 'path')

    def test_symlinks_cannot_escape_repository(self):
        self.repo.joinpath('new.md').symlink_to('/etc/hosts')
        self.assert_bad(self.run_check(['new.md']), 'path')

    def test_committed_snapshot_is_read_instead_of_uncommitted_content(self):
        self.write('new.md', '[good](target.md#first)\n')
        self.commit()
        self.write('new.md', '[dirty](missing.md)\n')
        self.assert_ok(self.run_check(['new.md'], worktree=False))

    def test_missing_base_is_disclosed(self):
        self.write('new.md', '# Intro\n')
        result = self.run_check(['new.md'], base=None, fallback_reason='first push')
        self.assert_ok(result)
        self.assertIn('UNVERIFIED', result.stdout)
        self.assertIn('base', result.stdout.lower())

    def test_json_fragment_is_not_claimed_as_gfm_anchor(self):
        self.write('data.json', '{"id":"F019"}')
        self.write('new.md', '[entry](data.json#F019)\n')
        result = self.run_check(['data.json', 'new.md'])
        self.assert_ok(result)
        self.assertIn('UNVERIFIED', result.stdout)

    def test_deleted_json_target_is_detected_even_with_unverified_fragment(self):
        self.write('data.json', '{"id":"F019"}')
        self.write('json-link.md', '[entry](data.json#F019)\n')
        self.base = self.commit()
        self.repo.joinpath('data.json').unlink()
        self.assert_bad(self.run_check([{'status':'D', 'path':'data.json'}]), 'data.json')


if __name__ == '__main__':
    unittest.main()
