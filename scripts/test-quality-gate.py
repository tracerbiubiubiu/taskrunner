#!/usr/bin/env python3
"""Independent behavior oracles for the three quality-gate tiers.

Fixtures describe the change's purpose, rather than repeating the classifier's
path rules. Git fixtures are isolated and never use the caller's index or config.
"""
import os
import json
import runpy
import subprocess
import sys
import tempfile
import unittest
from pathlib import Path
from unittest.mock import patch


GATE = runpy.run_path(str(Path(__file__).with_name('quality-gate.py')))
CLASSIFY = GATE['classify_changes']
BUILD = GATE['build_plan']
VALIDATE = GATE['validate_results']
KIND = 'core'
RUNTIME_JOBS = ['lint', 'unit', 'integration', 'acceptance', 'security']
HELPER_JOBS = ['helper-security']


def change(path, *, status='M', old_path=None, old_mode='100644', new_mode='100644'):
    item = {'path': path, 'status': status, 'old_mode': old_mode, 'new_mode': new_mode}
    if old_path is not None:
        item['old_path'] = old_path
    return item


class ClassificationTests(unittest.TestCase):
    def assert_mode(self, paths, mode, *, consumed_paths=()):
        changes = [change(path) if isinstance(path, str) else path for path in paths]
        plan = CLASSIFY(changes, KIND, list(consumed_paths))
        self.assertEqual(plan['mode'], mode, paths)
        self.assertEqual(plan['runtime'], mode == 'ordinary', paths)
        self.assertEqual(plan['changes'], changes)
        self.assertEqual(plan['kind'], KIND)
        self.assertIsInstance(plan['reasons'], list)
        if mode == 'reference':
            self.assertTrue(plan['references'], 'reference changes need an executable recipe')
        else:
            self.assertEqual(plan['references'], [])
        return plan

    def test_explanatory_markdown_does_not_require_runtime(self):
        for path in ('README.md', 'docs/design/future-architecture.md',
                     'docs/review/reports/phase5-audit.md'):
            with self.subTest(path=path):
                plan = self.assert_mode([path], 'document')
                self.assertFalse(plan['helper_security'])

    def test_registered_nonruntime_artifacts_need_reference_recipes(self):
        for path in ('docs/review/planning-backlog.json',
                     'docs/design/mcp-first-contract.schema.json',
                     'docs/design/examples/ai-soc-reference/fixtures.json',
                     'docs/design/examples/mcp-sdk-interop/go.mod',
                     'docs/phase5/prototypes/s1-form-designer.html',
                     'scripts/calculate-planning-schedule.py'):
            with self.subTest(path=path):
                self.assert_mode([path], 'reference')

    def test_known_schemas_keep_their_existing_behavioral_reference_oracles(self):
        for path, recipe in (
                ('docs/design/capability-interconnect-detail.schema.json', 'capability'),
                ('docs/design/domain-agent-reference-scenario.schema.json', 'ai-soc'),
                ('docs/design/skill-reference-package.schema.json', 'ai-soc'),
                ('docs/design/soc-collaboration-authorization.schema.json', 'ai-soc')):
            with self.subTest(path=path):
                plan = self.assert_mode([path], 'reference')
                self.assertIn('schema', plan['references'])
                self.assertIn(recipe, plan['references'])
        unknown = self.assert_mode(['docs/design/new-candidate.schema.json'], 'reference')
        self.assertEqual(unknown['references'], ['schema'])

    def test_product_and_gate_inputs_keep_runtime(self):
        for path in ('internal/service/ticket/create.go', 'src/tickets/editor.tsx',
                     'migrations/000100_new_table.up.sql', 'configs/config.yaml',
                     '.env.example', 'go.mod', 'go.sum', 'package-lock.json',
                     'Dockerfile', 'docs/swagger.json', 'docs/swagger.yaml', 'docs/docs.go',
                     'test/fixtures/tickets.json', 'internal/service/ticket/create_test.go',
                     '.github/workflows/ci.yml', '.githooks/pre-commit', 'AGENTS.md',
                     'scripts/quality-gate.py', 'scripts/test-quality-gate.py',
                     'docs/design/unregistered.json', 'docs/design/unregistered.html',
                     'docs/design/executable.sh',
                     'unknown/new-format.data'):
            with self.subTest(path=path):
                self.assert_mode([path], 'ordinary')

    def test_document_and_product_mixture_keeps_runtime(self):
        self.assert_mode(['README.md', 'internal/service/ticket/create.go'], 'ordinary')

    def test_reference_and_product_mixture_keeps_both_validation_sets(self):
        plan = CLASSIFY([change('docs/review/planning-backlog.json'),
                         change('internal/service/ticket/create.go')], KIND)
        self.assertEqual(plan['mode'], 'ordinary')
        self.assertTrue(plan['runtime'])
        self.assertTrue(plan['references'])

    def test_renaming_runtime_file_into_docs_cannot_drop_runtime(self):
        self.assert_mode([change('docs/design/example.md', status='R',
                                 old_path='internal/service/ticket/create.go')], 'ordinary')

    def test_markdown_mode_changes_cannot_disguise_executables_or_symlinks(self):
        for old_mode, new_mode in (('100644', '100755'), ('100755', '100644'),
                                   ('100644', '120000'), ('120000', '100644')):
            with self.subTest(old_mode=old_mode, new_mode=new_mode):
                self.assert_mode([change('docs/design/readme.md', old_mode=old_mode,
                                         new_mode=new_mode)], 'ordinary')

    def test_runtime_consumed_markdown_is_an_input_not_just_prose(self):
        self.assert_mode(['docs/design/prompt.md'], 'ordinary',
                         consumed_paths=['docs/design/prompt.md'])

    def test_real_helper_inputs_request_helper_security(self):
        for path in ('deployments/tools/migrate/go.mod', 'deployments/tools/rclone/go.sum',
                     'deployments/tools/migrate/Dockerfile', '.github/workflows/helper-tools.yml',
                     'go.mod'):
            with self.subTest(path=path):
                self.assertTrue(self.assert_mode([path], 'ordinary')['helper_security'])

    def test_readme_does_not_rebuild_helper_images(self):
        self.assertFalse(self.assert_mode(['README.md'], 'document')['helper_security'])

    def test_isolated_mcp_sdk_reference_does_not_rebuild_product_or_helper(self):
        plan = self.assert_mode(['docs/design/examples/mcp-sdk-interop/interop_test.go'], 'reference')
        self.assertFalse(plan['helper_security'])

    def test_repository_alias_is_normalized(self):
        for alias, expected in (('zhuzhao', 'core'), ('zhuzhao-ui', 'ui'),
                                ('zhuzhao-utils', 'utils')):
            with self.subTest(alias=alias):
                plan = CLASSIFY([change('README.md')], alias)
                self.assertEqual(plan['kind'], expected)

    def test_nested_agent_policy_is_ordinary_while_neighbor_readmes_stay_light(self):
        for path, mode in (
                ('docs/AGENTS.md', 'ordinary'),
                ('docs/design/examples/capability-reference/AGENTS.md', 'ordinary'),
                ('docs/README.md', 'document'),
                ('docs/design/examples/capability-reference/README.md', 'document')):
            with self.subTest(path=path):
                plan = self.assert_mode([path], mode)
                self.assertEqual(plan['helper_security'], mode == 'ordinary')


class GitWindowTests(unittest.TestCase):
    def setUp(self):
        self.temp = tempfile.TemporaryDirectory(prefix='quality-gate-test-')
        self.addCleanup(self.temp.cleanup)
        self.repo = Path(self.temp.name)
        self.env = {key: value for key, value in os.environ.items() if not key.startswith('GIT_')}
        self.env.update(GIT_CONFIG_NOSYSTEM='1', GIT_CONFIG_GLOBAL=os.devnull,
                        GIT_AUTHOR_NAME='Gate fixture', GIT_AUTHOR_EMAIL='gate-fixture@example.invalid',
                        GIT_COMMITTER_NAME='Gate fixture', GIT_COMMITTER_EMAIL='gate-fixture@example.invalid')
        self.git('init', '--quiet')
        self.write('README.md', 'initial docs\n')
        self.write('main.go', 'package main\n')
        self.base = self.commit('initial fixture', 'README.md', 'main.go')

    def git(self, *args):
        result = subprocess.run(['git', '-C', str(self.repo), *args], env=self.env,
                                text=True, capture_output=True, check=True)
        return result.stdout.strip()

    def write(self, path, content):
        target = self.repo / path
        target.parent.mkdir(parents=True, exist_ok=True)
        target.write_text(content)

    def commit(self, message, *paths):
        self.git('add', '--', *paths)
        self.git('-c', 'core.hooksPath=/dev/null', 'commit', '--quiet', '-m', message, '--', *paths)
        return self.git('rev-parse', 'HEAD')

    def test_complete_commit_window_includes_earlier_runtime_change(self):
        self.write('main.go', 'package main\nfunc main() {}\n')
        self.commit('runtime change first', 'main.go')
        self.write('README.md', 'documentation change last\n')
        head = self.commit('docs change second', 'README.md')
        plan = BUILD(self.repo, self.base, head, KIND)
        self.assertEqual(plan['mode'], 'ordinary')
        self.assertTrue(plan['runtime'])
        self.assertEqual({item['path'] for item in plan['changes']}, {'main.go', 'README.md'})
        self.assertEqual(plan['base'], self.base)
        self.assertEqual(plan['head'], head)
        self.assertTrue(plan['tree'])

    def test_local_document_window_includes_uncommitted_source(self):
        self.write('README.md', 'committed prose\n')
        head = self.commit('docs only commit', 'README.md')
        self.write('main.go', 'package main\nfunc main() {}\n')
        self.write('internal/new.go', 'package internal\n')
        plan = BUILD(self.repo, self.base, head, KIND, worktree=True)
        self.assertEqual(plan['mode'], 'ordinary')
        self.assertTrue(plan['runtime'])
        self.assertTrue({'main.go', 'internal/new.go'} <=
                        {item['path'] for item in plan['changes']})

    def test_local_reference_window_includes_staged_source(self):
        self.write('docs/review/planning-backlog.json', '{}\n')
        head = self.commit('reference only commit', 'docs/review/planning-backlog.json')
        self.write('main.go', 'package main\nfunc main() {}\n')
        self.git('add', '--', 'main.go')
        plan = BUILD(self.repo, self.base, head, KIND, worktree=True)
        self.assertEqual(plan['mode'], 'ordinary')
        self.assertIn('main.go', [item['path'] for item in plan['changes']])

    def test_initial_push_without_base_cannot_claim_document_exemption(self):
        plan = BUILD(self.repo, '0' * 40, self.base, KIND)
        self.assertEqual(plan['mode'], 'ordinary')
        self.assertTrue(plan['runtime'])

    def test_snapshot_digest_binds_staged_blob_even_when_worktree_is_unchanged(self):
        self.write('README.md', 'staged version A\n')
        self.git('add', '--', 'README.md')
        self.write('README.md', 'final working version\n')
        first = BUILD(self.repo, self.base, self.base, KIND, worktree=True)
        self.write('README.md', 'staged version B\n')
        self.git('add', '--', 'README.md')
        self.write('README.md', 'final working version\n')
        second = BUILD(self.repo, self.base, self.base, KIND, worktree=True)
        self.assertNotEqual(first['input_digest'], second['input_digest'])

    def test_git_rename_into_docs_preserves_runtime_origin(self):
        (self.repo / 'docs/design').mkdir(parents=True)
        self.git('mv', 'main.go', 'docs/design/example.md')
        self.git('-c', 'core.hooksPath=/dev/null', 'commit', '--quiet', '-m',
                 'move runtime file into docs', '--', 'main.go', 'docs/design/example.md')
        head = self.git('rev-parse', 'HEAD')
        plan = BUILD(self.repo, self.base, head, KIND)
        self.assertEqual(plan['mode'], 'ordinary')
        self.assertTrue(plan['runtime'])
        self.assertTrue(any(item.get('old_path') == 'main.go' for item in plan['changes']))

    def test_source_relative_js_url_keeps_consumed_markdown_on_runtime_gate(self):
        path = 'docs/design/prompt.md'
        self.write(path, 'initial runtime prompt\n')
        self.write('src/runtime.mjs', "import {readFileSync} from 'node:fs';\n"
                   "const prompt = readFileSync(new URL('../docs/design/prompt.md', import.meta.url), 'utf8');\n")
        base = self.commit('source-relative runtime input', path, 'src/runtime.mjs')
        self.write(path, 'changed runtime prompt\n')
        head = self.commit('change runtime input only', path)
        plan = BUILD(self.repo, base, head, KIND)
        self.assertEqual(plan['mode'], 'ordinary')
        self.assertTrue(plan['runtime'])
        self.assertIn(path, plan['consumed_paths'])

    def test_go_embedded_docs_directory_recursively_keeps_runtime_gate(self):
        path = 'docs/assets/nested/prompt.md'
        self.write(path, 'initial embedded prompt\n')
        self.write('main.go', 'package main\nimport "embed"\n'
                   '//go:embed docs\nvar content embed.FS\n')
        base = self.commit('embedded runtime input directory', path, 'main.go')
        self.write(path, 'changed embedded prompt\n')
        head = self.commit('change embedded input only', path)
        plan = BUILD(self.repo, base, head, KIND)
        self.assertEqual(plan['mode'], 'ordinary')
        self.assertTrue(plan['runtime'])
        self.assertIn(path, plan['consumed_paths'])

    def test_static_import_export_and_require_make_candidate_schemas_runtime_inputs(self):
        consumers = [
            ('import', 'mjs', "import schema from '../{path}' with {{type: 'json'}};\n"),
            ('export', 'mjs', "export {{default as schema}} from '../{path}' with {{type: 'json'}};\n"),
            ('require', 'cjs', "const schema = require('../{path}');\n"),
        ]
        for name, extension, content in consumers:
            with self.subTest(consumer=name):
                path = 'docs/design/' + name + '-candidate.schema.json'
                source = 'src/' + name + '-app.' + extension
                self.write(path, '{"type":"object"}\n')
                self.write(source, content.format(path=path))
                base = self.commit('static ' + name + ' consumer', path, source)
                self.write(path, '{"type":"object","additionalProperties":false}\n')
                head = self.commit('change static ' + name + ' input only', path)
                plan = BUILD(self.repo, base, head, KIND)
                self.assertEqual(plan['mode'], 'ordinary')
                self.assertTrue(plan['runtime'])
                self.assertIn(path, plan['consumed_paths'])

    def test_initial_push_cli_uses_origin_main_merge_base_when_available(self):
        self.git('update-ref', 'refs/remotes/origin/main', self.base)
        self.write('README.md', 'only this branch documentation changed\n')
        self.commit('new branch documentation', 'README.md')
        with tempfile.NamedTemporaryFile('w', suffix='.json') as event:
            json.dump({'before': '0' * 40}, event)
            event.flush()
            result = subprocess.run([sys.executable, str(Path(__file__).with_name('quality-gate.py').resolve()),
                                     'classify', '--kind', KIND, '--event-path', event.name],
                                    cwd=self.repo, env=self.env, text=True, capture_output=True)
            self.assertEqual(result.returncode, 0, result.stdout + result.stderr)
            plan = json.loads(result.stdout)
            self.assertEqual(plan['base'], self.base)
            self.assertEqual(plan['mode'], 'document')
            self.assertEqual({item['path'] for item in plan['changes']}, {'README.md'})
            self.assertIn('origin/main', plan['base_source'])
            self.git('update-ref', '-d', 'refs/remotes/origin/main')
            unavailable = subprocess.run([sys.executable, str(Path(__file__).with_name('quality-gate.py').resolve()),
                                          'classify', '--kind', KIND, '--event-path', event.name],
                                         cwd=self.repo, env=self.env, text=True, capture_output=True)
            self.assertEqual(unavailable.returncode, 0, unavailable.stdout + unavailable.stderr)
            self.assertEqual(json.loads(unavailable.stdout)['mode'], 'ordinary')


class FixtureIsolationTests(unittest.TestCase):
    def test_fixture_git_commands_cannot_use_the_callers_git_directory_or_index(self):
        caller = GitWindowTests('test_complete_commit_window_includes_earlier_runtime_change')
        # Only setup is used; this temporary repository represents the caller.
        caller.setUp()
        self.addCleanup(caller.doCleanups)
        head = caller.git('rev-parse', 'HEAD')
        index = (caller.repo / '.git/index').read_bytes()
        modules = [runpy.run_path(str(Path(__file__).with_name(name)))
                   for name in ('test-check-docs.py', 'test-check-references.py')]
        fixtures = [(GitWindowTests, 'test_complete_commit_window_includes_earlier_runtime_change'),
                    (modules[0]['DocumentChecks'], 'test_missing_plan_is_rejected'),
                    (modules[1]['ReferenceRunnerTests'], 'test_unknown_recipe_is_an_error')]
        with patch.dict(os.environ, {'GIT_DIR': str(caller.repo / '.git'),
                                     'GIT_WORK_TREE': str(caller.repo),
                                     'GIT_INDEX_FILE': str(caller.repo / '.git/index')}):
            for fixture_class, method in fixtures:
                with self.subTest(fixture=fixture_class.__name__):
                    fixture = fixture_class(method)
                    try:
                        fixture.setUp()
                        self.assertEqual(Path(fixture.git('rev-parse', '--show-toplevel')).resolve(),
                                         fixture.repo.resolve())
                    finally:
                        fixture.doCleanups()
        self.assertEqual(caller.git('rev-parse', 'HEAD'), head)
        self.assertEqual((caller.repo / '.git/index').read_bytes(), index)
        self.assertEqual(caller.git('status', '--porcelain'), '')


class ResultGuardTests(unittest.TestCase):
    def plan(self, mode='document', helper=False):
        path = {'document': 'README.md',
                'reference': 'docs/review/planning-backlog.json',
                'ordinary': 'internal/service/ticket/create.go'}[mode]
        paths = [change(path)]
        if helper:
            paths.append(change('deployments/tools/migrate/go.mod'))
        return CLASSIFY(paths, KIND)

    def results(self, plan):
        results = {key: 'success' for key in ('changes', 'commit-policy', 'document-check')}
        results['reference-check'] = 'success' if plan['references'] else 'skipped'
        results.update({job: 'success' if plan['runtime'] else 'skipped' for job in RUNTIME_JOBS})
        results.update({job: 'success' if plan['helper_security'] else 'skipped' for job in HELPER_JOBS})
        return results

    def errors(self, plan, results):
        return VALIDATE(plan, results, RUNTIME_JOBS, HELPER_JOBS)

    def test_each_tier_accepts_success_and_only_justified_skips(self):
        for mode in ('document', 'reference', 'ordinary'):
            with self.subTest(mode=mode):
                plan = self.plan(mode)
                self.assertEqual(self.errors(plan, self.results(plan)), [])

    def test_every_base_check_must_succeed(self):
        plan = self.plan()
        for key in ('changes', 'commit-policy', 'document-check'):
            for outcome in (None, 'failure', 'cancelled', 'skipped'):
                with self.subTest(key=key, outcome=outcome):
                    results = self.results(plan)
                    if outcome is None:
                        results.pop(key)
                    else:
                        results[key] = outcome
                    self.assertTrue(self.errors(plan, results))

    def test_reference_check_cannot_be_missing_failed_cancelled_or_skipped(self):
        plan = self.plan('reference')
        for outcome in (None, 'failure', 'cancelled', 'skipped'):
            with self.subTest(outcome=outcome):
                results = self.results(plan)
                if outcome is None:
                    results.pop('reference-check')
                else:
                    results['reference-check'] = outcome
                self.assertTrue(self.errors(plan, results))

    def test_each_runtime_job_is_required_for_ordinary_changes(self):
        plan = self.plan('ordinary')
        for job in RUNTIME_JOBS:
            for outcome in (None, 'failure', 'cancelled', 'skipped'):
                with self.subTest(job=job, outcome=outcome):
                    results = self.results(plan)
                    if outcome is None:
                        results.pop(job)
                    else:
                        results[job] = outcome
                    self.assertTrue(self.errors(plan, results))

    def test_helper_security_cannot_be_skipped_for_changed_helper_inputs(self):
        plan = self.plan('ordinary', helper=True)
        for outcome in (None, 'failure', 'cancelled', 'skipped'):
            with self.subTest(outcome=outcome):
                results = self.results(plan)
                if outcome is None:
                    results.pop('helper-security')
                else:
                    results['helper-security'] = outcome
                self.assertTrue(self.errors(plan, results))

    def test_result_failure_is_not_excused_by_document_tier(self):
        plan = self.plan()
        results = self.results(plan)
        results['unit'] = 'failure'
        self.assertTrue(self.errors(plan, results))

    def test_exempt_jobs_still_need_an_explicit_result(self):
        plan = self.plan()
        for job in ('unit', 'reference-check', 'helper-security'):
            with self.subTest(job=job):
                results = self.results(plan)
                results.pop(job)
                self.assertTrue(self.errors(plan, results))

    def test_tampered_document_label_cannot_exempt_runtime_source(self):
        plan = self.plan('ordinary')
        plan.update(mode='document', runtime=False)
        self.assertTrue(self.errors(plan, self.results(plan)))

    def test_inconsistent_runtime_flag_cannot_exempt_ordinary_mode(self):
        plan = self.plan('ordinary')
        plan['runtime'] = False
        self.assertTrue(self.errors(plan, self.results(plan)))

    def test_reference_recipe_cannot_be_deleted_to_skip_validation(self):
        plan = self.plan('reference')
        plan['references'] = []
        self.assertTrue(self.errors(plan, self.results(plan)))

    def test_helper_flag_cannot_be_cleared_to_skip_security(self):
        plan = self.plan('ordinary', helper=True)
        plan['helper_security'] = False
        self.assertTrue(self.errors(plan, self.results(plan)))


if __name__ == '__main__':
    unittest.main()
