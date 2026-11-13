# taskrunner

**简体中文 | [English](README.en.md)**

**把任务受理、定时调度和执行记录从业务服务中分离。**

taskrunner 是一个 Go 任务调度服务：先持久化任务，再通过 Redis/Asynq 投递，向固定的业务 HTTP 端点请求执行，并记录状态、尝试和失败。业务逻辑仍在能力属主的 handler 中，便于多个入口复用已有业务服务。

产品族：[zhuzhao](https://github.com/tracerbiubiubiu/zhuzhao) · [zhuzhao-ui](https://github.com/tracerbiubiubiu/zhuzhao-ui) · [activelist](https://github.com/tracerbiubiubiu/activelist) · **taskrunner** · [zhuzhao-utils](https://github.com/tracerbiubiubiu/zhuzhao-utils)

在 zhuzhao 中，它承接平台任务和业务计划；也可由可信服务通过 HTTP 接入。

## 当前能力

- **DB-first 受理**：先保存任务记录，Redis 暂不可用时保留待补投记录，由扫描器重试投递。
- **任务与计划**：HTTP 提交、任务定义、cron 调度、手动触发及运行记录查询。
- **执行管理**：固定目标 HTTP 回调、超时、重试、死信查询，以及符合当前状态的取消／重试操作。
- **并发与留痕**：任务 ID 去重、执行身份栅栏、请求关联和操作者归因。
- **独立部署**：PostgreSQL／SQLite 存储选择、Redis/Asynq 队列、AK/SK 签名与健康探针。

HTTP 入口、状态和准确限制见 [API 与回调契约](docs/api/contract.md)。

## 先体验一条完整执行链

仓库提供 [端到端演示脚本](scripts/e2e/e2e.sh)：启动真实 taskrunner 进程、专属 Redis 和合成回调接收方，通过签名 HTTP 请求验证受理、执行、重试及恢复。需要当前 [go.mod](go.mod) 要求的 Go、Make、Docker、Python 3 和 curl；宿主端口 **6397、6398、6399** 须空闲。

从仓库根目录执行：

```sh
make e2e

# 需要同时验证 cron 到点调度时，单独运行：
E2E_INCLUDE_CRON=1 make e2e
```

脚本打印逐项结果及汇总，失败时非零退出；退出时清理自己启动的进程和 Redis 容器，临时工作目录与日志保留供排查。它不要求已有 zhuzhao 实例，也不代表真实业务 handler 已完成接入。

## 运行自己的服务

先按 [开发指南](docs/guides/development.md)配置专属 Redis、数据库／SQLite 文件、队列、调用方密钥和回调目标，再从仓库根目录运行：

```sh
make build
./bin/taskrunner serve configs/config.yaml
```

默认 API 监听 **18080**；SQLite 模式仍然需要 Redis。另开终端检查实际监听地址：

```sh
curl --fail http://127.0.0.1:18080/healthz
curl --fail http://127.0.0.1:18080/readyz
```

探针正常返回 HTTP 200，ready 检查数据库和 Redis。业务路由位于 `/v1`，需要 AK/SK；调用方密钥与回调签名密钥分别配置，不能用未签名 curl 代替完整业务接入。生产 Compose 不发布宿主端口，接线与备份步骤见 [部署指南](docs/ops/deployment.md)。

## 执行语义与边界

**受理成功不等于执行成功，执行记录也不自动证明业务效果完成。** 当前回调是请求业务属主执行，不是把结果贴回工单的通知接口；在途回调的 2xx 完成语义及后续效果凭证见 [平台集成合同](docs/design/platform-integration.md)。执行具体动作的一方仍须保证业务幂等。

`dept`、`submitted_by` 和 `X-Operator` 是筛选／归因信息，不是终端用户数据权限。任务参数避免放入密码、令牌和敏感原文；当前执行记录不能当成私有秘密通道。

taskrunner 负责调度和执行账本，工单／风险状态、用户授权与专业处置由各自系统管理。统一能力目录、持续委托、结构化业务结果回贴及 AI/MCP 接线仍按主仓计划实施；当前没有通用工作流画布或 `/monitor` 页面。

## 文档与贡献

- [文档导航](docs/README.md) · [API 与回调](docs/api/contract.md)。
- [部署](docs/ops/deployment.md) · [备份恢复](docs/ops/backup.md)。
- [开发指南](docs/guides/development.md) · [协作协议](AGENTS.md)：测试先行，小范围 PR，保留失败与验证证据。

```sh
make hooks
make lint
make test
make build
```

PG 测试使用专属 `TASKRUNNER_TEST_PG_DSN`，E2E 使用上面的独立环境；不能用缺少依赖而跳过的测试证明真实集成通过。工具链／容器候选和迁移边界见 [版本升级记录](docs/ops/version-upgrade.md)，当前检出代码不等于 main 已发布或已部署。

欢迎在 [Issues](https://github.com/tracerbiubiubiu/taskrunner/issues)描述任务场景或可复现问题，或提交聚焦的 PR。

## 许可

[MIT](LICENSE)。
