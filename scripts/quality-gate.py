#!/usr/bin/env python3
"""Read-only candidate classification and fail-closed quality result aggregation."""
import argparse
import fnmatch
import hashlib
import json
import os
from pathlib import Path, PurePosixPath
import re
import subprocess
import sys

ALIASES = {'zhuzhao': 'core', 'zhuzhao-ui': 'ui', 'zhuzhao-utils': 'utils'}
KINDS = {'core', 'ui', 'taskrunner', 'activelist', 'utils'}
REFERENCE_ROOTS = {
    'docs/design/examples/capability-reference/': 'capability',
    'docs/design/examples/uaa-reference-mock/': 'uaa',
    'docs/design/examples/ai-soc-reference/': 'ai-soc',
    'docs/design/examples/mcp-sdk-interop/': 'mcp-sdk',
}
SCHEMA_ORACLES = {
    'docs/design/capability-interconnect-detail.schema.json': 'capability',
    'docs/design/domain-agent-reference-scenario.schema.json': 'ai-soc',
    'docs/design/skill-reference-package.schema.json': 'ai-soc',
    'docs/design/soc-collaboration-authorization.schema.json': 'ai-soc',
}
PLANNING_FILES = {
    'uaa-dual-mode-review', 'user-requirements-reconciliation',
    'local-core-release-estimate', 'uaa-implementation-estimate',
    'owner-decision-agenda', 'capability-interconnect-supplement',
    'research-supplement-status', 'legacy-trigger-disposition',
    'execution-schedule-model', 'execution-schedule',
    'uaa-real-integration-conditions', 'roadmap-capability-continuity',
    'post-baseline-scope-estimates', 'focus-depth-verification',
    'conversation-reaudit', 'first-usable-release-target',
}
PROTOTYPES = {'docs/design/prototypes/lifecycle-workspace.html',
              'docs/phase5/prototypes/s1-form-designer.html'}
GOVERNANCE = {'AGENTS.md', 'docs/standards.md', 'docs/review/planning-document-scope.json',
              'docs/guides/development.md', 'docs/guides/testing.md',
              'docs/design/engineering-execution-design.md'}


def safe_path(path):
    return isinstance(path, str) and bool(path) and not (
        path.startswith('/') or '\\' in path or '\n' in path or '\r' in path
        or any(p in ('..', '.') for p in path.split('/')))


def _path_policy(path, consumed):
    if not safe_path(path):
        return 'ordinary', [], 'invalid or nonportable path'
    if PurePosixPath(path).name == 'AGENTS.md':
        return 'ordinary', [], 'scoped collaboration policy authority'
    if path in consumed or path in GOVERNANCE or path.startswith('docs/api/'):
        return 'ordinary', [], 'runtime input or policy authority'
    if path == 'scripts/test-reference-prototypes.mjs':
        return 'ordinary', ['prototype'], 'checker requires full gates and self-test'
    if path in PROTOTYPES:
        return 'reference', ['prototype'], 'registered offline prototype'
    if path == 'docs/design/mcp-first-contract.schema.json':
        return 'reference', ['mcp-sdk'], '2020-12 MCP tool schemas validated by isolated SDK'
    if path in SCHEMA_ORACLES:
        return 'reference', ['schema', SCHEMA_ORACLES[path]], 'schema format and registered behavioral samples'
    if path == 'scripts/calculate-planning-schedule.py':
        return 'reference', ['planning'], 'registered non-runtime planning generator'
    for root, recipe in REFERENCE_ROOTS.items():
        if path.startswith(root):
            if PurePosixPath(path).name.lower() == 'readme.md':
                return 'document', [], 'reference documentation'
            return 'reference', [recipe], 'registered isolated reference example'
    if fnmatch.fnmatchcase(path, 'docs/design/*.schema.json') and path.count('/') == 2:
        return 'reference', ['schema'], 'candidate schema format only; no registered behavioral proof'
    if path.startswith('docs/review/') and path.count('/') == 2 and path.endswith('.json'):
        stem = PurePosixPath(path).stem
        if stem.startswith('planning-') or stem in PLANNING_FILES:
            return 'reference', ['planning'], 'registered planning data'
    if path == 'docs/design/capability-operation-catalog.json':
        return 'reference', ['capability'], 'registered capability catalog'
    if path.lower().endswith('.md') and (path.startswith('docs/') or '/' not in path):
        return 'document', [], 'explanatory Markdown'
    if path.startswith(('docs/assets/', 'docs/images/')) and PurePosixPath(path).suffix.lower() in ('.png', '.jpg', '.jpeg', '.gif', '.webp'):
        return 'document', [], 'static documentation image'
    return 'ordinary', [], 'runtime, validation infrastructure, or unregistered artifact'


def classify_changes(changes, kind, consumed_paths=()):
    kind = ALIASES.get(kind, kind)
    if kind not in KINDS:
        raise ValueError('unknown repository kind: ' + str(kind))
    consumed = set(consumed_paths)
    rank, refs, reasons, helper = 0, set(), [], False
    for item in changes:
        paths = [item.get('path')]
        if item.get('old_path'):
            paths.append(item['old_path'])
        modes_safe = all(item.get(key, '100644') in ('000000', '100644')
                         for key in ('old_mode', 'new_mode'))
        for path in paths:
            tier, recipes, reason = _path_policy(path, consumed)
            if not modes_safe:
                tier, reason = 'ordinary', 'executable or symlink mode'
            rank = max(rank, {'document': 0, 'reference': 1, 'ordinary': 2}[tier])
            refs.update(recipes)
            reasons.append(str(path) + ': ' + reason)
            if kind == 'core' and (path in ('go.mod', 'go.sum', 'Makefile', 'AGENTS.md', 'docs/standards.md')
                    or (isinstance(path, str) and (PurePosixPath(path).name == 'AGENTS.md'
                        or path.startswith(('.github/workflows/', '.githooks/', 'deployments/tools/', 'scripts/quality-', 'scripts/check-', 'scripts/test-quality-', 'scripts/test-check-'))))):
                helper = True
    mode = ('document', 'reference', 'ordinary')[rank]
    return {'schema_version': 1, 'kind': kind, 'mode': mode,
            'runtime': mode == 'ordinary', 'references': sorted(refs),
            'helper_security': helper, 'reasons': reasons or ['no changed files'],
            'consumed_paths': sorted(consumed), 'changes': changes}


def git(repo, *args):
    return subprocess.check_output(['git', '-C', str(repo), *args], stderr=subprocess.PIPE)


def _diff(repo, *revisions):
    raw = git(repo, 'diff', '--raw', '-z', '--no-abbrev', '--find-renames', *revisions, '--')
    tokens, items, i = raw.split(b'\0'), [], 0
    while i < len(tokens) and tokens[i]:
        fields = tokens[i].decode().split()
        i += 1
        path = tokens[i].decode('utf-8', 'surrogateescape')
        i += 1
        item = {'status': fields[4], 'path': path,
                'old_mode': fields[0][1:], 'new_mode': fields[1],
                'old_blob': fields[2], 'new_blob': fields[3]}
        if fields[4].startswith(('R', 'C')):
            item['old_path'] = path
            item['path'] = tokens[i].decode('utf-8', 'surrogateescape')
            i += 1
        items.append(item)
    return items


def _consumed_paths(repo):
    """Find supported literal inputs, rather than claim complete dependency analysis.

    Quoted local paths are normalized relative to runtime source files, covering
    static import/export/require and new URL without requiring an AST engine.
    A quoted docs directory conservatively consumes its descendant files; Go
    embed patterns are also recognized. Computed paths (for example a dynamic
    filepath.Join) and production inputs read by excluded scripts require an
    explicit entry in the governed set below; editing this gate stays ordinary.
    Callers of classify_changes can also supply their explicit consumed_paths.
    """
    from urllib.parse import unquote, urlsplit

    repo = Path(repo).resolve()
    consumed = {'docs/api/errcode.md', 'docs/swagger.json', 'docs/swagger.yaml', 'docs/docs.go'}

    def add_local_file(candidate):
        if candidate.is_symlink():
            return
        resolved = candidate.resolve()
        if resolved.is_relative_to(repo):
            consumed.add(resolved.relative_to(repo).as_posix())

    for raw in git(repo, 'ls-files', '-z').split(b'\0'):
        path = raw.decode('utf-8', 'surrogateescape')
        if not path.endswith(('.go', '.ts', '.tsx', '.js', '.jsx', '.cjs', '.mjs', '.vue')) or path.startswith(('docs/', 'scripts/')):
            continue
        target = repo / path
        if not target.is_file() or target.is_symlink():
            continue
        content = target.read_text(errors='replace')
        for match in re.findall(r'''["'`](docs/[^"'`\r\n]+)["'`]''', content):
            if safe_path(match):
                consumed.add(match)
        for _, literal in re.findall(r'''(["'`])([^"'`\r\n]+)\1''', content):
            if '${' in literal or '\\' in literal:
                continue
            try:
                parts = urlsplit(literal)
                if parts.scheme or parts.netloc or not parts.path:
                    continue
                candidate = (target.parent / unquote(parts.path)).resolve()
                if not candidate.is_relative_to(repo):
                    continue
                relative = candidate.relative_to(repo)
                if relative.parts and relative.parts[0] == 'docs':
                    if candidate.is_dir():
                        for nested in candidate.rglob('*'):
                            if nested.is_file():
                                add_local_file(nested)
                    else:
                        add_local_file(candidate)
                elif candidate.is_file():
                    add_local_file(candidate)
            except ValueError:
                # Malformed or computed literals need explicit registration.
                continue
        for patterns in re.findall(r'^\s*//go:embed\s+(.+)$', content, re.M):
            for pattern in re.findall(r'"[^"\r\n]*"|`[^`\r\n]*`|\S+', patterns):
                pattern = pattern.strip('"`')
                include_hidden = pattern.startswith('all:')
                if include_hidden:
                    pattern = pattern[4:]
                if not safe_path(pattern):
                    continue
                for match in target.parent.glob(pattern):
                    if match.is_file():
                        add_local_file(match)
                    elif match.is_dir() and not match.is_symlink():
                        for nested in match.rglob('*'):
                            if nested.is_file() and (include_hidden or not any(
                                    part.startswith(('.', '_')) for part in nested.relative_to(match).parts)):
                                add_local_file(nested)
    return sorted(consumed)


def _digest(repo, plan):
    inputs = {key: plan[key] for key in ('base', 'head', 'tree', 'worktree', 'changes', 'consumed_paths')}
    h = hashlib.sha256(json.dumps(inputs, sort_keys=True).encode())
    if plan['worktree']:
        for path in sorted({p for c in plan['changes'] for p in (c['path'], c.get('old_path')) if p}):
            if not safe_path(path):
                raise ValueError('invalid candidate path')
            file = repo / path
            h.update(path.encode('utf-8', 'surrogateescape'))
            if file.is_symlink():
                h.update(b'link:' + os.readlink(file).encode())
            elif file.is_file():
                h.update(str(file.stat().st_mode & 0o777).encode())
                h.update(file.read_bytes())
            else:
                h.update(b'deleted')
    return h.hexdigest()


def build_plan(repo, base, head, kind, worktree=False):
    repo = Path(repo).resolve()
    resolved_head = git(repo, 'rev-parse', '--verify', head + '^{commit}').decode().strip()
    fallback = None
    try:
        if not base or set(base) == {'0'}:
            raise ValueError('missing base')
        resolved_base = git(repo, 'rev-parse', '--verify', base + '^{commit}').decode().strip()
        changes = _diff(repo, resolved_base, resolved_head)
    except (subprocess.CalledProcessError, ValueError):
        resolved_base = None
        fallback = 'base unavailable: ordinary gates required; no exemption inferred'
        empty = git(repo, 'hash-object', '-t', 'tree', '--stdin').decode().strip()
        changes = _diff(repo, empty, resolved_head)
    if worktree:
        changes.extend(_diff(repo, '--cached', resolved_head))
        changes.extend(_diff(repo))
        for raw in git(repo, 'ls-files', '--others', '--exclude-standard', '-z').split(b'\0'):
            if not raw:
                continue
            path = raw.decode('utf-8', 'surrogateescape')
            target = repo / path
            mode = '120000' if target.is_symlink() else ('100755' if target.stat().st_mode & 0o111 else '100644')
            changes.append({'path': path, 'status': 'A', 'old_mode': '000000', 'new_mode': mode})
    changes = [json.loads(key) for key in sorted({json.dumps(c, sort_keys=True) for c in changes})]
    plan = classify_changes(changes, kind, _consumed_paths(repo))
    if fallback:
        plan.update(mode='ordinary', runtime=True, helper_security=plan['kind'] == 'core', fallback_reason=fallback)
        plan['reasons'].append(fallback)
    plan.update(base=resolved_base, head=resolved_head,
                tree=git(repo, 'rev-parse', resolved_head + '^{tree}').decode().strip(),
                worktree=bool(worktree))
    plan['input_digest'] = _digest(repo, plan)
    return plan


def verify_plan_snapshot(plan, repo):
    """Reject stale plans, dirty CI checkouts, or a different candidate window."""
    repo = Path(repo).resolve()
    if git(repo, 'rev-parse', 'HEAD').decode().strip() != plan.get('head'):
        raise ValueError('plan HEAD differs from current checkout')
    if not plan.get('worktree') and git(repo, 'status', '--porcelain', '--untracked-files=all').strip():
        raise ValueError('committed plan requires a clean checkout')
    actual = build_plan(repo, plan.get('base'), plan['head'], plan['kind'], plan.get('worktree', False))
    for key in ('schema_version', 'base', 'head', 'tree', 'worktree', 'input_digest',
                'changes', 'consumed_paths', 'mode', 'runtime', 'references', 'helper_security'):
        if actual.get(key) != plan.get(key):
            raise ValueError('candidate plan mismatch: ' + key)
    return actual


def validate_results(plan, results, runtime_jobs, helper_jobs=()):
    errors = []
    try:
        expected = classify_changes(plan['changes'], plan['kind'], plan.get('consumed_paths', ()))
        if plan.get('fallback_reason'):
            expected.update(mode='ordinary', runtime=True, helper_security=plan['kind'] == 'core')
        for field in ('schema_version', 'mode', 'runtime', 'references', 'helper_security'):
            if plan.get(field) != expected[field]:
                errors.append('inconsistent classification: ' + field)
    except (KeyError, TypeError, ValueError) as exc:
        return ['invalid plan: ' + str(exc)]
    required = {'changes', 'commit-policy', 'document-check'}
    if plan['references']:
        required.add('reference-check')
    if plan['runtime']:
        required.update(runtime_jobs)
    if plan['helper_security']:
        required.update(helper_jobs)
    for job in sorted(required | {'reference-check'} | set(runtime_jobs) | set(helper_jobs)):
        result = results.get(job)
        if result != 'success' and (job in required or result != 'skipped'):
            errors.append(job + ': ' + str(result) + (' (success required)' if job in required else ' (explicit success/skipped required)'))
    return errors


def _env_json(name):
    value = os.environ.get(name)
    if not value:
        raise ValueError('missing environment variable: ' + name)
    return json.loads(value)


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    subs = parser.add_subparsers(dest='command', required=True)
    classify = subs.add_parser('classify')
    classify.add_argument('--kind', required=True)
    classify.add_argument('--event-path')
    classify.add_argument('--base')
    classify.add_argument('--head', default='HEAD')
    classify.add_argument('--github-output')
    summarize = subs.add_parser('summarize')
    summarize.add_argument('--plan-env', required=True)
    summarize.add_argument('--needs-env', required=True)
    summarize.add_argument('--runtime-jobs', required=True)
    summarize.add_argument('--helper-jobs', default='')
    local = subs.add_parser('local')
    local.add_argument('--kind', required=True)
    local.add_argument('--base', default=os.environ.get('QUALITY_BASE', 'origin/main'))
    local.add_argument('--plan-only', action='store_true')
    args = parser.parse_args()
    repo = Path(git(Path.cwd(), 'rev-parse', '--show-toplevel').decode().strip())
    if args.command == 'summarize':
        plan = _env_json(args.plan_env)
        verify_plan_snapshot(plan, repo)
        needs = _env_json(args.needs_env)
        errors = validate_results(plan, {k: v.get('result') for k, v in needs.items()},
                                  args.runtime_jobs.split(','), [x for x in args.helper_jobs.split(',') if x])
        if errors:
            raise ValueError('; '.join(errors))
        print('quality-gate: ' + plan['mode'] + ' requirements satisfied for ' + plan['head'])
        return
    base = args.base
    base_source = 'argument' if base else 'unavailable'
    if args.command == 'classify' and args.event_path:
        event = json.loads(Path(args.event_path).read_text())
        pull_request_base = event.get('pull_request', {}).get('base', {}).get('sha')
        before = event.get('before')
        base = pull_request_base or before or base
        base_source = 'pull-request-base' if pull_request_base else ('event-before' if before else base_source)
        if base and set(base) == {'0'}:
            try:
                resolved_head = git(repo, 'rev-parse', '--verify', args.head + '^{commit}').decode().strip()
                base = git(repo, 'merge-base', 'origin/main', resolved_head).decode().strip()
                base_source = 'origin/main-merge-base'
            except subprocess.CalledProcessError:
                base_source = 'event-before-unavailable'
    plan = build_plan(repo, base, args.head if args.command == 'classify' else 'HEAD', args.kind,
                      worktree=args.command == 'local')
    plan['base_source'] = base_source
    print(json.dumps(plan, ensure_ascii=True, indent=2))
    if args.command == 'classify' and args.github_output:
        with open(args.github_output, 'a') as out:
            for key in ('mode', 'runtime', 'helper_security'):
                out.write(key + '=' + (str(plan[key]).lower() if isinstance(plan[key], bool) else plan[key]) + '\n')
            out.write('references=' + str(bool(plan['references'])).lower() + '\n')
            out.write('plan=' + json.dumps(plan, separators=(',', ':'), ensure_ascii=True) + '\n')
    if args.command == 'local' and not args.plan_only:
        env = dict(os.environ, QUALITY_PLAN=json.dumps(plan))
        for script in ('check-docs.py', 'check-references.py'):
            subprocess.run([sys.executable, str(repo / 'scripts' / script), '--plan-env', 'QUALITY_PLAN'], cwd=repo, env=env, check=True)
        if plan['runtime']:
            raise ValueError('ordinary change: run the original full gates in an isolated environment; '
                             'quality-plan is not full-gate evidence (see development guide)')
        print('local ' + plan['mode'] + ' checks passed; no product runtime gates required')


if __name__ == '__main__':
    try:
        main()
    except (ValueError, OSError, subprocess.CalledProcessError) as exc:
        print('quality-gate: ' + str(exc), file=sys.stderr)
        sys.exit(1)
