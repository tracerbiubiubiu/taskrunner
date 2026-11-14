# taskrunner

**[简体中文](README.md) | English**

**Separate task admission, scheduling and execution records from business services.**

taskrunner is a Go task service. It persists tasks before delivering them through Redis/Asynq, requests execution at a configured business HTTP endpoint, and records status, attempts and failures. Business handlers remain in the owning service, so multiple entry points can reuse existing application logic.

Project family: [zhuzhao](https://github.com/tracerbiubiubiu/zhuzhao) · [zhuzhao-ui](https://github.com/tracerbiubiubiu/zhuzhao-ui) · [activelist](https://github.com/tracerbiubiubiu/activelist) · **taskrunner** · [zhuzhao-utils](https://github.com/tracerbiubiubiu/zhuzhao-utils)

It handles platform tasks and schedules for zhuzhao and also accepts HTTP calls from trusted services.

## Available today

- **DB-first admission:** tasks are saved before enqueueing. When Redis is temporarily unavailable, a scanner can retry delivery from pending records.
- **Tasks and schedules:** HTTP submission, job definitions, cron schedules, manual triggers and run queries.
- **Execution management:** callbacks to a fixed HTTP target, timeouts, retries, dead-letter queries and state-dependent cancellation/retry.
- **Concurrency and attribution:** task-ID deduplication, execution identity fencing, request correlation and operator attribution.
- **Separate deployment:** PostgreSQL or SQLite storage, Redis/Asynq queues, AK/SK signatures and health probes.

See the [HTTP and callback contract](docs/api/contract.md) for exact inputs, states and limits.

## Try a complete execution chain

The [end-to-end script](scripts/e2e/e2e.sh) starts a real taskrunner process, a dedicated Redis container and a synthetic callback receiver. Signed HTTP requests exercise admission, execution, retries and recovery. Install the Go version required by [go.mod](go.mod), Make, Docker, Python 3 and curl. Host ports **6397, 6398 and 6399** must be free.

From the repository root:

```sh
make e2e

# Run separately to include due-time cron scheduling:
E2E_INCLUDE_CRON=1 make e2e
```

The script reports individual checks and a summary, exiting nonzero on failure. It removes its own processes and Redis container on exit and retains the temporary directory and logs for diagnosis. It needs no running zhuzhao instance and does not establish that a real business handler has been integrated.

## Run your own service

Use the [development guide](docs/guides/development.md) to configure a dedicated Redis instance, database/SQLite file, queue, caller credentials and callback target. Then run from the repository root:

```sh
make build
./bin/taskrunner serve configs/config.yaml
```

The default API port is **18080**. SQLite mode still requires Redis. Check the actual listen address from another terminal:

```sh
curl --fail http://127.0.0.1:18080/healthz
curl --fail http://127.0.0.1:18080/readyz
```

Healthy probes return HTTP 200; readiness checks the database and Redis. Business routes under `/v1` require AK/SK authentication. Configure incoming caller keys and the outgoing callback key separately; an unsigned curl request is not a complete business integration. Production Compose does not publish a host port. See [deployment](docs/ops/deployment.md) for connectivity and backup instructions.

## Execution semantics and scope

**Admission is not execution success, and an execution record does not automatically prove the business effect completed.** A callback requests execution by the owning service; it is not a ticket-result notification endpoint. The current in-flight callback/2xx completion limitation and planned effect receipts are documented in the [integration contract](docs/design/platform-integration.md). The business handler still owns effect idempotency.

`dept`, `submitted_by` and `X-Operator` are filters or attribution, not end-user data authorization. Keep passwords, tokens and sensitive source content out of task parameters; current run records are not a private secret channel.

taskrunner owns scheduling and execution records. Ticket/risk state, user authorization and specialist security actions remain with their owners. The shared capability catalog, persistent delegation, structured business-result projection and AI/MCP integration are planned work. There is currently no general workflow canvas or `/monitor` page.

## Documentation and contributing

- [Documentation](docs/README.md) · [HTTP and callbacks](docs/api/contract.md).
- [Deployment](docs/ops/deployment.md) · [Backup and recovery](docs/ops/backup.md).
- [Development guide](docs/guides/development.md) · [Collaboration rules](AGENTS.md): tests first, focused PRs and explicit validation evidence.

```sh
make hooks
make lint
make test
make build
```

PG tests use a dedicated `TASKRUNNER_TEST_PG_DSN`; E2E uses the isolated environment above. Tests skipped for missing dependencies do not prove integration success. See [version upgrades](docs/ops/version-upgrade.md) for candidate toolchains, containers and migration boundaries. A checked-out candidate is distinct from a main release or deployment.

Share a task use case or reproduction in [Issues](https://github.com/tracerbiubiubiu/taskrunner/issues), or contribute a focused PR.

## License

[MIT](LICENSE).
