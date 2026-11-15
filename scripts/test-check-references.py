#!/usr/bin/env python3
"""Reference checks must execute fixed tools and reject stale or false proof."""
import hashlib
import io
import json
import os
import runpy
import subprocess
import tempfile
import unittest
from pathlib import Path
from contextlib import redirect_stdout
from unittest.mock import patch


CHECKER = runpy.run_path(str(Path(__file__).with_name('check-references.py')))
RUN = CHECKER['run_references']
HASH_ERRORS = CHECKER['validate_message_hash']
BUILD = runpy.run_path(str(Path(__file__).with_name('quality-gate.py')))['build_plan']


class ReferenceRunnerTests(unittest.TestCase):
    def setUp(self):
        self.temp = tempfile.TemporaryDirectory(prefix='reference-gate-test-')
        self.addCleanup(self.temp.cleanup)
        self.repo = Path(self.temp.name).resolve()
        self.env = {key: value for key, value in os.environ.items() if not key.startswith('GIT_')}
        self.env.update(GIT_CONFIG_NOSYSTEM='1', GIT_CONFIG_GLOBAL=os.devnull,
                        GIT_AUTHOR_NAME='Reference fixture', GIT_AUTHOR_EMAIL='reference@example.invalid',
                        GIT_COMMITTER_NAME='Reference fixture', GIT_COMMITTER_EMAIL='reference@example.invalid')
        self.git('init', '--quiet')
        files = {'README.md': 'Reference fixture\n',
                 'Makefile': 'planning-check:\n\t@true\n',
                 'docs/review/planning-backlog.json': '{}\n',
                 'docs/design/examples/mcp-sdk-interop/go.mod': 'module reference.invalid/sdk\n',
                 'docs/phase5/prototypes/s1-form-designer.html': '<html><script>let a = 1;</script></html>\n'}
        for path, content in files.items():
            target = self.repo / path
            target.parent.mkdir(parents=True, exist_ok=True)
            target.write_text(content)
        self.git('add', '--', *files)
        self.git('-c', 'core.hooksPath=/dev/null', 'commit', '--quiet', '-m', 'reference fixtures', '--', *files)
        self.head = self.git('rev-parse', 'HEAD')
        self.calls = []

    def git(self, *args):
        return subprocess.check_output(['git', '-C', str(self.repo), *args], env=self.env, text=True).strip()

    def plan(self, references, path='docs/review/planning-backlog.json', kind='core'):
        base = self.git('rev-parse', 'HEAD')
        target = self.repo / path
        target.parent.mkdir(parents=True, exist_ok=True)
        if path.endswith('.schema.json'):
            target.write_text('{"$schema":"http://json-schema.org/draft-07/schema#","type":"object"}\n')
        elif path.endswith('.json'):
            target.write_text('{"fixture_changed":true}\n')
        else:
            target.write_text((target.read_text() if target.exists() else '// reference checker\n') + '\n')
        self.git('add', '--', path)
        self.git('-c', 'core.hooksPath=/dev/null', 'commit', '--quiet', '-m', 'candidate reference', '--', path)
        plan = BUILD(self.repo, base, 'HEAD', kind)
        plan['references'] = references
        return plan

    def runner(self, command, **kwargs):
        self.calls.append((command, kwargs))
        return subprocess.CompletedProcess(command, 0, '', '')

    def test_planning_executes_the_registered_check_without_plan_commands(self):
        plan = self.plan(['planning'])
        plan['command'] = 'touch should-never-exist'
        self.assertEqual(RUN(plan, self.repo, self.runner), [])
        self.assertEqual([call[0] for call in self.calls], [['make', 'planning-check']])
        self.assertEqual(Path(self.calls[0][1]['cwd']), self.repo)
        self.assertFalse(self.calls[0][1].get('shell', False))

    def test_stale_head_is_rejected_before_tool_execution(self):
        plan = self.plan(['planning'])
        plan['head'] = '0' * 40
        self.assertTrue(RUN(plan, self.repo, self.runner))
        self.assertEqual(self.calls, [])

    def test_unbound_dirty_checkout_is_rejected(self):
        plan = self.plan(['planning'])
        (self.repo / 'README.md').write_text('new unbound input\n')
        self.assertTrue(RUN(plan, self.repo, self.runner))
        self.assertEqual(self.calls, [])

    def test_unknown_recipe_is_an_error(self):
        self.assertTrue(RUN(self.plan(['shell: touch marker']), self.repo, self.runner))
        self.assertEqual(self.calls, [])

    def test_recipe_failure_cannot_be_reported_as_success(self):
        def failing(command, **kwargs):
            return subprocess.CompletedProcess(command, 2, '', 'fixture failure')
        self.assertTrue(RUN(self.plan(['planning']), self.repo, failing))

    def test_sdk_runs_only_its_isolated_go_module(self):
        path = 'docs/design/examples/mcp-sdk-interop/go.mod'
        self.assertEqual(RUN(self.plan(['mcp-sdk'], path), self.repo, self.runner), [])
        self.assertTrue(self.calls, 'SDK reference must execute a real check')
        self.assertEqual(self.calls[0][0], ['go', 'test', '-race', './...'])
        self.assertEqual(Path(self.calls[0][1]['cwd']), self.repo / 'docs/design/examples/mcp-sdk-interop')

    def test_schema_validation_requires_explicit_pinned_ajv_directory(self):
        with patch.dict(os.environ, {'AJV_MODULE': ''}):
            errors = RUN(self.plan(['schema'], 'docs/design/generic-candidate.schema.json'),
                         self.repo, self.runner)
            self.assertTrue(any('AJV_MODULE' in error for error in errors), errors)

    def test_mixed_mcp_bundle_is_not_sent_to_draft7_compiler(self):
        bundle = 'docs/design/mcp-first-contract.schema.json'
        schema = 'docs/design/generic-candidate.schema.json'
        for name, document in {
            bundle: {'schema_dialect': 'https://json-schema.org/draft/2020-12/schema', 'tools': []},
            schema: {'$schema': 'http://json-schema.org/draft-07/schema#', 'type': 'object'},
        }.items():
            target = self.repo / name
            target.parent.mkdir(parents=True, exist_ok=True)
            target.write_text(json.dumps(document) + '\n')
        self.git('add', '--', bundle, schema)
        self.git('-c', 'core.hooksPath=/dev/null', 'commit', '--quiet', '-m', 'mixed schema references', '--', bundle, schema)
        plan = BUILD(self.repo, self.head, 'HEAD', 'core')
        self.assertEqual(plan['references'], ['mcp-sdk', 'schema'])
        with tempfile.TemporaryDirectory(prefix='reference-ajv-package-') as module:
            Path(module, 'package.json').write_text('{"name":"ajv","version":"6.15.0"}\n')
            with patch.dict(os.environ, {'AJV_MODULE': module}):
                self.assertEqual(RUN(plan, self.repo, self.runner), [])
        self.assertEqual(self.calls[0][0], ['go', 'test', '-race', './...'])
        self.assertEqual(Path(self.calls[0][1]['cwd']), self.repo / 'docs/design/examples/mcp-sdk-interop')
        compiler = self.calls[1][0]
        self.assertEqual(compiler[:4], ['node', '-e', CHECKER['SCHEMA_CHECK'], 'schemas'])
        self.assertEqual(compiler[4:], [str(self.repo / schema)], 'MCP bundle must be checked only by its 2020-12 SDK recipe')

    def test_unknown_dialect_candidate_is_still_sent_for_explicit_rejection(self):
        path = 'docs/design/unregistered-dialect.schema.json'
        plan = self.plan(['schema'], path)
        target = self.repo / path
        target.write_text('{"$schema":"https://json-schema.org/draft/2020-12/schema","type":"object"}\n')
        self.git('add', '--', path)
        self.git('-c', 'core.hooksPath=/dev/null', 'commit', '--quiet', '-m', 'unknown schema dialect', '--', path)
        plan = BUILD(self.repo, self.head, 'HEAD', 'core')
        with tempfile.TemporaryDirectory(prefix='reference-ajv-package-') as module:
            Path(module, 'package.json').write_text('{"name":"ajv","version":"6.15.0"}\n')
            with patch.dict(os.environ, {'AJV_MODULE': module}):
                self.assertEqual(RUN(plan, self.repo, self.runner), [])
        self.assertIn(str(target), self.calls[0][0][4:], 'unknown dialect must reach validator rather than silently disappear')

    def test_success_keeps_tool_summary_and_output_without_inline_verifier_source(self):
        plan = self.plan(['planning'])
        output = io.StringIO()
        def successful(command, **kwargs):
            return subprocess.CompletedProcess(command, 0, 'checked 7 synthetic cases\n', 'synthetic tool note\n')
        with redirect_stdout(output):
            self.assertEqual(RUN(plan, self.repo, successful), [])
        log = output.getvalue()
        self.assertIn('make planning-check', log)
        self.assertIn('cwd=', log)
        self.assertIn('checked 7 synthetic cases', log)
        self.assertIn('synthetic tool note', log)

    def test_missing_prototype_interaction_checker_cannot_claim_success(self):
        path = 'docs/phase5/prototypes/s1-form-designer.html'
        self.assertTrue(RUN(self.plan(['prototype'], path), self.repo, self.runner))

    def test_changed_prototype_checker_runs_positive_and_self_test(self):
        path = 'scripts/test-reference-prototypes.mjs'
        self.assertEqual(RUN(self.plan(['prototype'], path), self.repo, self.runner), [])
        self.assertEqual([call[0] for call in self.calls], [
            ['node', path], ['node', path, '--self-test']])

    def test_runner_cannot_modify_bound_inputs_and_still_pass(self):
        def mutating(command, **kwargs):
            (self.repo / 'README.md').write_text('unexpected mutation\n')
            return subprocess.CompletedProcess(command, 0, '', '')
        self.assertTrue(RUN(self.plan(['planning']), self.repo, mutating))


class MessageHashTests(unittest.TestCase):
    def document(self):
        fields = ['delivery_id', 'channel_profile_ref', 'recipient_ref', 'template_version',
                  'subject', 'body', 'business_ref', 'source_event_ref', 'rule_id',
                  'rule_version', 'operation_id', 'expires_at']
        value = {key: key + '-value' for key in fields}
        value.update(rule_version=1, source_event_ref={'source': 'reference', 'event_id': 'reference-1'},
                     subject='Synthetic é', body='Synthetic e\u0301\ntrailing space: ')
        digest = hashlib.sha256(json.dumps(value, ensure_ascii=False, sort_keys=True,
                                          separators=(',', ':')).encode()).hexdigest()
        value['payload_sha256'] = digest
        return {'hash_reference': {'case_id': 'SEND-FROZEN-PAYLOAD', 'projection_fields': fields,
                                   'sha256': digest},
                'cases': [{'id': 'SEND-FROZEN-PAYLOAD', 'value': value}]}

    def test_frozen_message_projection_hash_is_recomputed(self):
        document = self.document()
        self.assertEqual(HASH_ERRORS(document), [])
        document['cases'][0]['value']['body'] = 'changed content'
        self.assertTrue(HASH_ERRORS(document))

    def test_removing_a_hash_projection_field_cannot_hide_content(self):
        document = self.document()
        document['hash_reference']['projection_fields'].remove('body')
        self.assertTrue(HASH_ERRORS(document))


if __name__ == '__main__':
    unittest.main()
