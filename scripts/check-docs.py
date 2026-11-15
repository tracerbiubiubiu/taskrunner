#!/usr/bin/env python3
"""Read-only changed-document checks using Python's standard library and Git.

Changed Markdown is checked in full. Unchanged inbound links are checked only
when they were valid at the base and the candidate change breaks their target.
No network requests or product test commands are performed.
"""
import argparse
import html
import json
import os
from pathlib import Path, PurePosixPath
import posixpath
import re
import subprocess
import sys
import unicodedata
from urllib.parse import unquote

REPOSITORIES = {'zhuzhao', 'zhuzhao-ui', 'activelist', 'taskrunner', 'zhuzhao-utils'}
MARKDOWN = {'.md', '.markdown', '.mdown'}
SHA = re.compile(r'^[0-9a-fA-F]{40}$')


class CheckError(ValueError):
    pass


def unique_object(pairs):
    result = {}
    for key, value in pairs:
        if key in result:
            raise CheckError(f'duplicate JSON key {key!r}')
        result[key] = value
    return result


def parse_json(text):
    def invalid_constant(value):
        raise CheckError(f'invalid JSON constant {value}')
    try:
        return json.loads(text, object_pairs_hook=unique_object, parse_constant=invalid_constant)
    except json.JSONDecodeError as exc:
        raise CheckError(f'invalid JSON: {exc}') from exc


def git(root, *args, allow_missing=False):
    result = subprocess.run(['git', *args], cwd=root, stdout=subprocess.PIPE,
                            stderr=subprocess.PIPE)
    if result.returncode:
        if allow_missing:
            return None
        raise CheckError('git ' + ' '.join(args[:2]) + ': ' +
                         result.stderr.decode('utf-8', 'replace').strip())
    return result.stdout


def valid_path(path):
    if not isinstance(path, str) or not path or '\x00' in path or '\\' in path:
        raise CheckError(f'invalid plan path {path!r}')
    pure = PurePosixPath(path)
    if pure.is_absolute() or '..' in pure.parts or path in {'.', ''}:
        raise CheckError(f'plan path escapes repository: {path!r}')
    if pure.as_posix() != path:
        raise CheckError(f'noncanonical plan path: {path!r}')
    return path


def load_plan(root, variable):
    raw = os.environ.get(variable)
    if not raw:
        raise CheckError(f'plan environment {variable!r} is missing or empty')
    plan = parse_json(raw)
    if not isinstance(plan, dict):
        raise CheckError('plan must be a JSON object')
    head = plan.get('head')
    if not isinstance(head, str) or not SHA.fullmatch(head):
        raise CheckError('plan head must be a complete commit SHA')
    actual = git(root, 'rev-parse', 'HEAD').decode().strip()
    if head.lower() != actual.lower():
        raise CheckError(f'plan head {head} does not match current candidate HEAD {actual}')
    if type(plan.get('worktree')) is not bool:
        raise CheckError('plan worktree must be a boolean')
    base = plan.get('base')
    if base is not None:
        if not isinstance(base, str) or not SHA.fullmatch(base):
            raise CheckError('plan base must be a complete commit SHA or null')
        git(root, 'cat-file', '-e', base + '^{commit}')
    elif plan.get('mode') != 'ordinary' or not plan.get('fallback_reason'):
        raise CheckError('a missing base requires ordinary mode and an explicit fallback_reason')
    changes = plan.get('changes')
    if not isinstance(changes, list):
        raise CheckError('plan changes must be an array')
    for entry in changes:
        if not isinstance(entry, dict) or not isinstance(entry.get('status'), str):
            raise CheckError('plan change must contain status and path')
        if not re.fullmatch(r'[ACDMRTUX?][0-9]*', entry['status']):
            raise CheckError(f'invalid plan change status {entry["status"]!r}')
        valid_path(entry.get('path'))
        if entry.get('old_path') is not None:
            valid_path(entry['old_path'])
    return plan


class Snapshot:
    def __init__(self, root, revision=None, worktree=False):
        self.root, self.revision, self.worktree = root, revision, worktree
        self.cache = {}
        self.anchor_cache = {}

    def safe_local(self, name):
        path = self.root / name
        try:
            path.resolve().relative_to(self.root)
        except ValueError as exc:
            raise CheckError(f'path escapes repository through symlink: {name}') from exc
        return path

    def read(self, name):
        valid_path(name)
        if name in self.cache:
            return self.cache[name]
        if self.worktree:
            path = self.safe_local(name)
            try:
                data = path.read_bytes() if path.is_file() else None
            except OSError as exc:
                raise CheckError(f'cannot read path {name}: {exc}') from exc
        else:
            mode = git(self.root, 'ls-tree', self.revision, '--', name)
            if mode and mode.startswith(b'120000 '):
                raise CheckError(f'cannot validate committed symlink path {name}')
            data = git(self.root, 'show', self.revision + ':' + name, allow_missing=True)
        if data is not None:
            try:
                data = data.decode('utf-8-sig')
            except UnicodeDecodeError as exc:
                raise CheckError(f'path is not UTF-8 text: {name}') from exc
        self.cache[name] = data
        return data

    def exists(self, name):
        valid_path(name)
        if self.worktree:
            return self.safe_local(name).exists()
        return git(self.root, 'cat-file', '-e', self.revision + ':' + name,
                   allow_missing=True) is not None

    def paths(self):
        if self.worktree:
            raw = git(self.root, 'ls-files', '-z', '--cached', '--others', '--exclude-standard')
        else:
            raw = git(self.root, 'ls-tree', '-r', '-z', '--name-only', self.revision)
        return sorted(set(p.decode('utf-8') for p in raw.split(b'\0') if p))

    def anchors(self, name):
        if name not in self.anchor_cache:
            text = self.read(name)
            self.anchor_cache[name] = markdown_anchors(text) if text is not None else set()
        return self.anchor_cache[name]


def without_fences(text):
    lines, fence, length = [], None, 0
    for line in re.sub(r'<!--.*?-->', '', text, flags=re.S).splitlines():
        match = re.match(r'^\s{0,3}(`{3,}|~{3,})(.*)$', line)
        if fence:
            if match and match[1][0] == fence and len(match[1]) >= length and not match[2].strip():
                fence = None
            lines.append('')
        elif match:
            fence, length = match[1][0], len(match[1])
            lines.append('')
        elif line.startswith('    ') or line.startswith('\t'):
            lines.append('')
        else:
            lines.append(line)
    return '\n'.join(lines)


def heading_text(text):
    text = re.sub(r'!?(\[([^\]]*)\])\([^)]*\)', r'\2', text)
    text = re.sub(r'!?\[([^\]]*)\]\[[^\]]*\]', r'\1', text)
    text = re.sub(r'<[^>]*>', '', text)
    text = text.replace('`', '').replace('*', '').replace('~', '')
    text = re.sub(r'(?<!\w)_([^_]+)_(?!\w)', r'\1', text)
    return html.unescape(text)


def markdown_anchors(text):
    clean = without_fences(text)
    ids = set(re.findall(r'<[^>]+\b(?:id|name)\s*=\s*[\"\']([^\"\']+)[\"\']', clean, flags=re.I))
    headings = []
    lines = clean.splitlines()
    for index, line in enumerate(lines):
        atx = re.match(r'^\s{0,3}#{1,6}(?:[ \t]+(.*?)|[ \t]*)$', line)
        if atx:
            headings.append((index, re.sub(r'[ \t]+#+[ \t]*$', '', atx[1] or '')))
        elif (index and re.match(r'^\s{0,3}(?:=+|-+)\s*$', line) and lines[index-1].strip()
              and not re.match(r'^\s{0,3}(?:#{1,6}(?:\s|$)|>|[-+*]\s|\d+[.)]\s)', lines[index-1])):
            headings.append((index-1, lines[index-1].strip()))
    used = set()
    for _, heading in sorted(headings):
        title = heading_text(heading).lower()
        slug = ''.join(c for c in title if c in '_-' or
                       unicodedata.category(c)[0] not in {'P', 'S', 'C'})
        slug = re.sub(r'\s', '-', slug)
        original, suffix = slug, 0
        while slug in used:
            suffix += 1
            slug = original + '-' + str(suffix)
        used.add(slug)
    return ids | used


def destination(text, offset):
    while offset < len(text) and text[offset].isspace():
        offset += 1
    if offset >= len(text):
        return ''
    if text[offset] == '<':
        end = text.find('>', offset + 1)
        return text[offset+1:end] if end >= 0 else ''
    output, depth = [], 0
    while offset < len(text):
        char = text[offset]
        if char == '\\' and offset + 1 < len(text):
            output.append(text[offset+1]); offset += 2; continue
        if char == '(':
            depth += 1
        elif char == ')':
            if not depth:
                break
            depth -= 1
        elif char.isspace() and not depth:
            break
        output.append(char)
        offset += 1
    return ''.join(output)


def markdown_links(text):
    clean = without_fences(text)
    clean = re.sub(r'(`+).*?\1', '', clean, flags=re.S)
    links = []
    for match in re.finditer(r'!?\[[^\]\n]*\]\(', clean):
        links.append(destination(clean, match.end()))
    for match in re.finditer(r'^\s{0,3}\[[^\]\n]+\]:\s*', clean, re.M):
        links.append(destination(clean, match.end()))
    links.extend(html.unescape(match[1]) for match in
                 re.finditer(r'<[^>]+\b(?:href|src)\s*=\s*[\"\']([^\"\']+)[\"\']', clean, re.I))
    return list(dict.fromkeys(value for value in links if value))


def local_target(source, href):
    href = html.unescape(href.strip())
    if re.match(r'^[A-Za-z][A-Za-z0-9+.-]*:', href) or href.startswith('//'):
        return None, None, 'external'
    path, _, fragment = href.partition('#')
    path = unquote(path.split('?', 1)[0])
    fragment = unquote(fragment)
    if '\x00' in path or '\\' in path:
        raise CheckError(f'invalid local link path {href!r}')
    target = posixpath.normpath(path.lstrip('/') if path.startswith('/') else
                                posixpath.join(posixpath.dirname(source), path)) if path else source
    if target == '..' or target.startswith('../'):
        parts = PurePosixPath(target).parts
        if len(parts) >= 3 and parts[0] == '..' and parts[1] in REPOSITORIES and '..' not in parts[2:]:
            return target, fragment, 'cross-repository'
        raise CheckError(f'local link path escapes repository: {href!r}')
    return target, fragment, 'local'


def check_link(snapshot, source, href):
    """Return target, failure or None, and an explicit verification limitation."""
    target, fragment, scope = local_target(source, href)
    if scope == 'external':
        return None, None, 'external links not fetched'
    if scope == 'cross-repository':
        return target, None, f'cross-repository link not verified: {href}'
    if target == '.':
        if fragment:
            return target, f'directory has no verified anchor: {href}', None
        return target, None, None
    if not snapshot.exists(target):
        return target, f'missing local target: {href}', None
    if fragment:
        if Path(target).suffix.lower() in MARKDOWN:
            if fragment not in snapshot.anchors(target):
                return target, f'missing Markdown anchor: {href}', None
        else:
            return target, None, f'non-Markdown fragment not verified: {href}'
    return target, None, None


def check_documents(root, plan):
    current = Snapshot(root, plan['head'], plan['worktree'])
    previous = Snapshot(root, plan['base']) if plan['base'] else None
    paths = {change['path'] for change in plan['changes']}
    deleted = {change['path'] for change in plan['changes'] if change['status'].startswith('D')}
    affected = paths | {change['old_path'] for change in plan['changes'] if change.get('old_path')}
    errors, limitations = [], set()
    checked_markdown = checked_json = inbound = 0
    if previous is None:
        limitations.add('base unavailable: historical inbound-link comparison not performed')
    for path in sorted(paths):
        suffix = Path(path).suffix.lower()
        if suffix not in MARKDOWN | {'.json'}:
            continue
        try:
            text = current.read(path)
            if text is None:
                # Deletions are validated through their existing inbound links below.
                if path not in deleted:
                    errors.append(f'{path}: missing non-deleted changed path')
                continue
            if suffix == '.json':
                parse_json(text)
                checked_json += 1
            else:
                checked_markdown += 1
                for href in markdown_links(text):
                    _, failure, limitation = check_link(current, path, href)
                    if failure:
                        errors.append(f'{path}: {failure}')
                    if limitation:
                        limitations.add(f'{path}: {limitation}')
        except (CheckError, OSError) as exc:
            errors.append(f'{path}: {exc}')
    if previous is not None and affected:
        for source in previous.paths():
            if source in paths or Path(source).suffix.lower() not in MARKDOWN:
                continue
            try:
                old_text = previous.read(source)
                if old_text is None or not current.exists(source):
                    continue
                for href in markdown_links(old_text):
                    try:
                        target, _, scope = local_target(source, href)
                        if scope != 'local' or target not in affected:
                            continue
                        _, old_failure, old_limit = check_link(previous, source, href)
                        if old_failure:
                            continue
                        if old_limit:
                            # Unsupported fragment semantics do not excuse deleting
                            # a target file whose existence was verified at the base.
                            limitations.add(f'{source}: base {old_limit}')
                    except CheckError:
                        # A baseline path escape, unsupported link, or old symlink
                        # was never verified valid and is not candidate-created debt.
                        continue
                    try:
                        _, failure, limitation = check_link(current, source, href)
                        inbound += int(old_limit is None)
                        if failure:
                            errors.append(f'{source}: previously valid inbound link broken by candidate: {failure}')
                        if limitation:
                            limitations.add(f'{source}: {limitation}')
                    except CheckError as exc:
                        errors.append(f'{source}: {exc}')
            except (CheckError, OSError):
                # Unchanged, pre-existing unsupported content is not new debt.
                continue
    for limitation in sorted(limitations):
        print('UNVERIFIED: ' + limitation)
    for error in sorted(set(errors)):
        print('ERROR: ' + error, file=sys.stderr)
    print(f'document-check: Markdown={checked_markdown}, JSON={checked_json}, '
          f'previously-valid inbound links={inbound}, errors={len(set(errors))}')
    return 1 if errors else 0


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--plan-env', required=True)
    args = parser.parse_args()
    try:
        root = Path(git(Path.cwd(), 'rev-parse', '--show-toplevel').decode().strip()).resolve()
        return check_documents(root, load_plan(root, args.plan_env))
    except (CheckError, OSError) as exc:
        print('ERROR: document-check: ' + str(exc), file=sys.stderr)
        return 1


if __name__ == '__main__':
    sys.exit(main())
