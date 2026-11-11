#!/usr/bin/env python3
"""Commit-message policy; Python standard library + Git, no repository mutations.

All new commits, including merges, require Conventional Commit subjects and a
nonempty body. Automatic windows exclude a fixed, tracked adoption baseline;
explicit historical audits use ``range --strict``. Missing objects fail closed.
Only ``ci --fetch-missing`` may fetch exact event/baseline objects from origin.
"""

import argparse
import json
import os
from pathlib import Path
import re
import subprocess
import sys


SHA = re.compile(r"(?:[0-9a-fA-F]{40}|[0-9a-fA-F]{64})\Z")
SUBJECT = re.compile(
    r"(?:feat|fix|docs|refactor|test|chore|build|ci|perf)"
    r"(?:\([a-z0-9_-]+\))?: (?P<summary>[^\n]*)\Z")
BASELINE_FILE = Path(__file__).resolve().with_name("commit-policy-baseline")


class PolicyError(Exception):
    """Configuration, object availability, or Git execution failure."""


def git(*args, input_text=None, optional=False):
    # Keep GIT_INDEX_FILE: pathspec commits deliberately use a temporary index.
    result = subprocess.run(["git", *args], input=input_text, text=True,
                            encoding="utf-8", capture_output=True)
    if result.returncode and not optional:
        raise PolicyError("git " + args[0] + " failed: " + result.stderr.strip())
    return result


def validate_sha(value):
    if not isinstance(value, str) or not SHA.fullmatch(value):
        raise PolicyError("expected a full 40/64-digit hexadecimal object ID")
    return value.lower()


def is_zero(value):
    return set(validate_sha(value)) == {"0"}


def commit_oid(value, fetch_missing=False):
    value = validate_sha(value)
    if is_zero(value):
        raise PolicyError("zero object ID cannot identify a commit")
    result = git("rev-parse", "--verify", "--end-of-options",
                 value + "^{commit}", optional=True)
    if result.returncode and fetch_missing:
        # Exact validated OID only; never execute an event/ref as shell text.
        fetched = git("fetch", "--no-tags", "origin", value, optional=True)
        if fetched.returncode:
            raise PolicyError("cannot fetch required object " + value)
        result = git("rev-parse", "--verify", "--end-of-options",
                     value + "^{commit}", optional=True)
    if result.returncode:
        raise PolicyError("missing/non-commit object " + value +
                          "; fetch complete history or that exact object")
    return validate_sha(result.stdout.strip())


def check_message(message):
    """Return existing hard-rule violations; Unicode length is locale-free."""
    lines = message.replace("\r\n", "\n").split("\n")
    subject = lines[0]
    match = SUBJECT.fullmatch(subject)
    reasons = []
    if not match or not match.group("summary").strip():
        reasons.append("[1] subject must be type(scope): nonempty summary")
    if match:
        summary = match.group("summary")
        if len(summary) > 50:
            reasons.append("[2] summary %d chars (limit 50)" % len(summary))
        if summary.endswith((".", "。", "。）")):
            reasons.append("[3] summary ends with a period")
    if len(lines) < 2 or lines[1].strip():
        reasons.append("[4] no blank line between subject and body")
    if not any(line.strip() for line in lines[2:]):
        reasons.append("[5] body empty or whitespace-only")
    return reasons


def baseline_oid(args):
    if getattr(args, "strict", False):
        return None
    value = args.baseline
    if value is None:
        try:
            value = Path(args.baseline_file).read_text(encoding="utf-8").strip()
        except OSError as exc:
            raise PolicyError("cannot read adoption baseline: " + str(exc))
    return commit_oid(value, getattr(args, "fetch_missing", False))


def select_commits(before, after, baseline, fetch_missing=False):
    # Validate before too: deletion must not turn malformed input into a pass.
    before, after = validate_sha(before), validate_sha(after)
    if is_zero(after):
        return []
    after = commit_oid(after, fetch_missing)
    exclusions = []
    if not is_zero(before):
        exclusions.append(commit_oid(before, fetch_missing))
    if baseline:
        exclusions.append(baseline)
    command = ["rev-list", "--reverse", after]
    if exclusions:
        command.extend(["--not", *exclusions])
    return git(*command).stdout.splitlines()


def check_commits(commits):
    failures = 0
    for oid in dict.fromkeys(commits):
        message = git("show", "-s", "--format=%B", oid).stdout
        reasons = check_message(message)
        if reasons:
            failures += 1
            print("[FAIL] " + oid[:12] + ": " + "; ".join(reasons))
    count = len(set(commits))
    if failures:
        print("[FAIL] commit-policy: %d/%d commits violate rules 1-5" %
              (failures, count))
        return 1
    print("[OK] commit-policy: %d commits checked (empty window is valid)" % count)
    return 0


def main(argv=None):
    parser = argparse.ArgumentParser(description=__doc__)
    sub = parser.add_subparsers(dest="mode", required=True)
    message_parser = sub.add_parser("message", help="check proposed commit file")
    message_parser.add_argument("file", help="message path, or - for standard input")
    message_parser.add_argument("--raw", action="store_true",
                                help="stored Git message: do not strip editor comments")
    for mode in ("range", "pending", "pre-push", "ci"):
        p = sub.add_parser(mode)
        baseline = p.add_mutually_exclusive_group()
        baseline.add_argument("--baseline")
        baseline.add_argument("--baseline-file", default=str(BASELINE_FILE))
        if mode == "range":
            p.add_argument("--before", required=True)
            p.add_argument("--after", required=True)
            baseline.add_argument("--strict", action="store_true")
        elif mode == "pre-push":
            p.add_argument("remote", nargs="?")
            p.add_argument("url", nargs="?")
        elif mode == "ci":
            p.add_argument("--event-file", default=os.environ.get("GITHUB_EVENT_PATH"))
            p.add_argument("--event-name", default=os.environ.get("GITHUB_EVENT_NAME"))
            p.add_argument("--fetch-missing", action="store_true")
    args = parser.parse_args(argv)
    if args.mode == "message":
        raw = (sys.stdin.read() if args.file == "-" else
               Path(args.file).read_text(encoding="utf-8"))
        # Optional editor cleanup is a tool convenience, not a way to replace
        # an invalid original subject with a later non-comment line. Hooks use
        # --raw because Git -F preserves literal Markdown/comment lines; final
        # stored text is independently checked by pre-push and CI.
        cleaned = raw if args.raw else git(
            "stripspace", "--strip-comments", input_text=raw).stdout
        reasons = check_message(cleaned)
        if not args.raw:
            for reason in check_message(raw):
                if reason.startswith(("[1]", "[2]", "[3]")) and reason not in reasons:
                    reasons.append(reason)
        for reason in reasons:
            print("[FAIL] " + reason)
        return int(bool(reasons))

    if git("rev-parse", "--is-shallow-repository").stdout.strip() != "false":
        raise PolicyError("shallow history cannot establish an exact window; fetch --unshallow")
    baseline = baseline_oid(args)
    fetch_missing = getattr(args, "fetch_missing", False)
    pairs = []
    if args.mode == "range":
        pairs.append((args.before, args.after))
    elif args.mode == "pending":
        branch = git("symbolic-ref", "--quiet", "HEAD").stdout.strip()
        remote = "refs/remotes/origin/" + branch[len("refs/heads/"):]
        old = git("rev-parse", "--verify", "--end-of-options", remote,
                  optional=True)
        after = git("rev-parse", "--verify", "HEAD").stdout.strip()
        pairs.append((old.stdout.strip() if old.returncode == 0 else "0" * len(after),
                      after))
    elif args.mode == "pre-push":
        for line in sys.stdin:
            fields = line.split()
            if len(fields) != 4:
                raise PolicyError("malformed pre-push input; expected four fields")
            _, local_oid, _, remote_oid = fields
            pairs.append((remote_oid, local_oid))
    else:
        if not args.event_file:
            raise PolicyError("CI event file is required")
        event = json.loads(Path(args.event_file).read_text(encoding="utf-8"))
        if args.event_name == "push":
            pairs.append((event["before"], event["after"]))
        elif args.event_name == "pull_request":
            pr = event["pull_request"]
            pairs.append((pr["base"]["sha"], pr["head"]["sha"]))
        else:
            raise PolicyError("unsupported CI event: " + str(args.event_name))
    commits = []
    for before, after in pairs:
        commits.extend(select_commits(before, after, baseline, fetch_missing))
    if baseline:
        print("[INFO] adoption baseline ancestors excluded: " + baseline)
    else:
        print("[INFO] strict historical audit; no baseline exemptions")
    return check_commits(commits)


if __name__ == "__main__":
    try:
        sys.exit(main())
    except (PolicyError, OSError, UnicodeError, ValueError, KeyError, TypeError) as exc:
        print("[FAIL] commit-policy: " + str(exc), file=sys.stderr)
        sys.exit(2)
