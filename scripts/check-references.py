#!/usr/bin/env python3
"""Run registered, read-only reference recipes for a Git-bound quality plan."""
import argparse
import hashlib
import json
import os
import runpy
import subprocess
import sys
from pathlib import Path, PurePosixPath


RECIPES = {'planning', 'schema', 'capability', 'uaa', 'ai-soc', 'mcp-sdk', 'prototype'}
MESSAGE_FIELDS = ['delivery_id', 'channel_profile_ref', 'recipient_ref', 'template_version',
                  'subject', 'body', 'business_ref', 'source_event_ref', 'rule_id',
                  'rule_version', 'operation_id', 'expires_at']

# Source is controlled by this file. Candidate paths are separate argv values.
SCHEMA_CHECK = r'''
const fs = require('node:fs');
const path = require('node:path');
const moduleDir = process.env.AJV_MODULE;
if (!moduleDir || !path.isAbsolute(moduleDir)) throw new Error('AJV_MODULE must be an explicit absolute directory');
const pkg = JSON.parse(fs.readFileSync(path.join(moduleDir, 'package.json'), 'utf8'));
if (pkg.name !== 'ajv' || pkg.version !== '6.15.0') throw new Error('Expected pinned AJV 6.15.0');
const Ajv = require(moduleDir);
const mode = process.argv[1];
const paths = process.argv.slice(2);
const ajv = new Ajv({allErrors: true, jsonPointers: true, schemaId: 'auto'});
const schemas = paths.filter(p => p.endsWith('.schema.json')).map(p => JSON.parse(fs.readFileSync(p, 'utf8')));
for (const schema of schemas) {
  if (schema.$schema !== 'http://json-schema.org/draft-07/schema#') throw new Error('Expected Draft7 schema');
  if (!ajv.validateSchema(schema)) throw new Error(JSON.stringify(ajv.errors));
  ajv.addSchema(schema);
}
for (const schema of schemas) ajv.compile(schema);
if (mode === 'vectors') {
  for (const filename of paths.filter(p => !p.endsWith('.schema.json'))) {
    const document = JSON.parse(fs.readFileSync(filename, 'utf8'));
    const schema = JSON.parse(fs.readFileSync(filename.replace(/\.json$/, '.schema.json'), 'utf8'));
    const envelope = ajv.getSchema(schema.$id);
    if (!envelope(document)) throw new Error(filename + ': invalid vector envelope: ' + JSON.stringify(envelope.errors));
    for (const test of document.cases) {
      if (typeof test.expected_valid !== 'boolean' || !Object.hasOwn(schema.definitions, test.definition)) throw new Error('Invalid vector expectation/definition');
      const validate = ajv.compile({$ref: schema.$id + '#/definitions/' + test.definition});
      if (validate(test.value) !== test.expected_valid) throw new Error(filename + ': unexpected validity for ' + test.id);
    }
  }
}
console.log(JSON.stringify({draft7_schemas: schemas.length, vector_documents: mode === 'vectors' ? paths.length - schemas.length : 0}));
'''


def git(repo, *args):
    return subprocess.check_output(['git', '-c', 'core.fsmonitor=false', '-C', str(repo), *args],
                                   stderr=subprocess.PIPE).decode()


def safe_path(repo, name):
    relative = PurePosixPath(name)
    if relative.is_absolute() or '..' in relative.parts or not relative.parts:
        raise ValueError('unsafe candidate path: ' + name)
    result = repo / name
    if result.is_symlink() or not result.resolve().is_relative_to(repo.resolve()):
        raise ValueError('reference path must be a repository file: ' + name)
    return result


def unique_object(pairs):
    result = {}
    for key, value in pairs:
        if key in result:
            raise ValueError('duplicate JSON key: ' + key)
        result[key] = value
    return result


def read_json(path):
    def invalid_constant(value):
        raise ValueError('non-JSON numeric constant: ' + value)
    value = json.loads(path.read_text(), object_pairs_hook=unique_object,
                       parse_constant=invalid_constant)
    # Reject lone surrogates even when escaped in otherwise valid JSON.
    json.dumps(value, ensure_ascii=False).encode('utf-8')
    return value


def snapshot(repo):
    digest = hashlib.sha256()
    for args in (('rev-parse', 'HEAD'), ('status', '--porcelain=v1', '-z'),
                 ('diff', '--cached', '--binary', '--no-ext-diff')):
        digest.update(git(repo, *args).encode())
    names = git(repo, 'ls-files', '-z', '--cached', '--others', '--exclude-standard').split('\0')
    for name in sorted(set(names)):
        if not name:
            continue
        target = repo / name
        digest.update(name.encode())
        if target.is_symlink():
            digest.update(b'symlink' + os.readlink(target).encode())
        elif target.is_file():
            digest.update(str(target.stat().st_mode).encode() + target.read_bytes())
        else:
            digest.update(b'missing')
    return digest.hexdigest()


def validate_binding(plan, repo):
    gate = runpy.run_path(str(Path(__file__).with_name('quality-gate.py')))
    gate['verify_plan_snapshot'](plan, repo)


def run_references(plan, repo, runner=subprocess.run):
    repo = Path(repo).resolve()
    errors = []
    try:
        validate_binding(plan, repo)
        references = plan.get('references')
        if not isinstance(references, list) or any(recipe not in RECIPES for recipe in references):
            raise ValueError('unknown reference recipe')
        before = snapshot(repo)
        changes = plan.get('changes', [])
        for item in changes:
            if item.get('status') != 'D' and item['path'].endswith('.json'):
                read_json(safe_path(repo, item['path']))

        def execute(command, cwd=repo, env=None):
            display = command
            if command[:2] == ['node', '-e']:
                display = ['node', '-e', '<registered-schema-verifier>', *command[3:]]
            print('[reference] ' + ' '.join(display) + ' (cwd=' + str(Path(cwd).relative_to(repo)) + ')', flush=True)
            result = runner(command, cwd=cwd, env=env, text=True, capture_output=True,
                            timeout=900, shell=False)
            if result.returncode:
                output = (result.stdout or '') + (result.stderr or '')
                errors.append('reference command failed: ' + ' '.join(command[:2]) + '\n' + output[-6000:])
            else:
                for output in (result.stdout, result.stderr):
                    if output:
                        print(output.rstrip(), flush=True)

        def ajv_directory():
            value = os.environ.get('AJV_MODULE', '')
            if not value or not Path(value).is_absolute() or not Path(value).is_dir():
                raise ValueError('AJV_MODULE must name the explicit pinned AJV 6.15.0 directory')
            package = read_json(Path(value) / 'package.json')
            if package.get('name') != 'ajv' or package.get('version') != '6.15.0':
                raise ValueError('Expected pinned AJV 6.15.0')
            return value

        for recipe in sorted(set(references)):
            if recipe == 'planning':
                if plan.get('kind') == 'core':
                    execute(['make', 'planning-check'])
                # Other repositories have no planning executor: JSON syntax was checked above.
            elif recipe == 'schema':
                ajv_directory()
                paths = [str(safe_path(repo, item['path'])) for item in changes
                         if item.get('status') != 'D' and item['path'].endswith('.schema.json')
                         and item['path'] != 'docs/design/mcp-first-contract.schema.json']
                if paths:
                    # Register neighboring candidate schemas as well, so local $id references resolve.
                    # The precise MCP contract bundle uses 2020-12 per-tool schemas
                    # and is routed to its SDK verifier, never compiled as a Draft7 root.
                    neighbors = set()
                    for path in (repo / 'docs/design').rglob('*.schema.json'):
                        candidate = safe_path(repo, str(path.relative_to(repo)))
                        schema = read_json(candidate)
                        if isinstance(schema, dict) and schema.get('$schema') == 'http://json-schema.org/draft-07/schema#':
                            neighbors.add(str(candidate))
                    execute(['node', '-e', SCHEMA_CHECK, 'schemas', *sorted(set(paths) | neighbors)])
            elif recipe == 'capability':
                ajv_directory()
                directory = repo / 'docs/design/examples/capability-reference'
                execute(['node', str(directory / 'verify.cjs')])
                paths = []
                for name in ('module-profile-vectors', 'message-http-vectors'):
                    paths.extend([str(directory / (name + '.schema.json')),
                                  str(directory / (name + '.json'))])
                execute(['node', '-e', SCHEMA_CHECK, 'vectors', *paths])
                errors.extend(validate_message_hash(read_json(directory / 'message-http-vectors.json')))
            elif recipe == 'uaa':
                execute([sys.executable, str(repo / 'docs/design/examples/uaa-reference-mock/verify.py')])
            elif recipe == 'ai-soc':
                module = ajv_directory()
                directory = repo / 'docs/design/examples/ai-soc-reference'
                execute([sys.executable, str(directory / 'verify.py')])
                execute(['node', str(directory / 'validate-schemas.cjs'), module])
            elif recipe == 'mcp-sdk':
                directory = repo / 'docs/design/examples/mcp-sdk-interop'
                env = dict(os.environ, GOWORK='off', GOFLAGS='-mod=readonly')
                execute(['go', 'test', '-race', './...'], cwd=directory, env=env)
            elif recipe == 'prototype':
                verifier = safe_path(repo, 'scripts/test-reference-prototypes.mjs')
                if not verifier.is_file():
                    raise ValueError('no registered executable prototype interaction verifier is available')
                command = ['node', 'scripts/test-reference-prototypes.mjs']
                execute(command)
                if any(item['path'] == 'scripts/test-reference-prototypes.mjs' for item in changes):
                    execute(command + ['--self-test'])
        if snapshot(repo) != before:
            errors.append('reference checks modified bound repository inputs')
        validate_binding(plan, repo)
    except (ValueError, KeyError, TypeError, OSError, subprocess.SubprocessError) as error:
        errors.append(str(error))
    return errors


def validate_message_hash(document):
    try:
        reference = document['hash_reference']
        if reference['projection_fields'] != MESSAGE_FIELDS:
            return ['message hash projection fields differ from the frozen contract']
        rows = [item for item in document['cases'] if item['id'] == reference['case_id']]
        if len(rows) != 1:
            return ['message hash reference must select exactly one frozen case']
        value = rows[0]['value']
        projection = {key: value[key] for key in MESSAGE_FIELDS}
        payload = json.dumps(projection, ensure_ascii=False, sort_keys=True,
                             separators=(',', ':'), allow_nan=False).encode('utf-8')
        digest = hashlib.sha256(payload).hexdigest()
        if digest != reference['sha256'] or digest != value['payload_sha256']:
            return ['frozen message payload SHA256 does not match its exact projection']
        return []
    except (KeyError, TypeError, ValueError, UnicodeError) as error:
        return ['invalid message hash reference: ' + str(error)]


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument('--plan-env', default='QUALITY_PLAN')
    args = parser.parse_args()
    try:
        plan = json.loads(os.environ[args.plan_env], object_pairs_hook=unique_object)
    except (KeyError, ValueError) as error:
        print('invalid quality plan: ' + str(error), file=sys.stderr)
        return 1
    errors = run_references(plan, Path.cwd())
    for error in errors:
        print(error)
    if not errors:
        print('reference checks passed for ' + ', '.join(plan.get('references', [])))
    return int(bool(errors))


if __name__ == '__main__':
    raise SystemExit(main())
