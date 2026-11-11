# HTTP 与回调契约

本页描述当前候选实现，路由依据 `internal/handler/server.go` 的 `New`，绑定结构与查询解析也在该文件。业务状态依据 `internal/service/task_service.go`。只读文档迁移不表示版本已合并或部署。

## 接入与响应

业务路由为 `/v1`，仅内部网络并使用 utils `aksk.GinMiddleware` + `response.AKSKFail()`；探针 `/healthz`、`/readyz` 免鉴权且不返回内部依赖详情。响应采用 `{code,message,data,request_id}`，成功 code=0；错误保留 HTTP 状态。AK/SK 使用实际 pin 对应协议，query 不入签、时间窗不等于 nonce 单次消费。身份断言 `X-Operator` 和链路 `X-Request-ID` 纳入内部签名。

| 方法与路径 | 输入与输出 |
|---|---|
| `POST /v1/tasks` | `action` 必填；可选 `task_id`、`request_id`、`dept`、`params`、`submitted_by`、`source_ip`、`timeout_secs`。持久化成功即受理，Redis 暂不可用仍200待补投；不代表执行成功。相同 task_id 终身去重。 |
| `GET /v1/tasks/:id` | 按 task_id 查询现态，包含 dept 标签。 |
| `GET /v1/runs` | `id/request_id/action/status/job_id/dept/submitted_by/from/to/page/page_size`；dept 可重复多值，按运行快照列筛选，不 JOIN 当前 jobs；from/to 为 RFC3339。返回 list/total/page/page_size。 |
| `GET /v1/jobs` | `dept/action_id/enabled/page/page_size`；返回分页任务定义。 |
| `POST /v1/jobs` | `action_id`、`trigger_type` 必填；可选 cron_spec/params/dept/enabled/timeout_secs/description/created_by/owner_service。 |
| `POST /v1/jobs/update` | body `job_id` 定位；可更新定义字段，省略表示保留。 |
| `POST /v1/jobs/trigger` | body `job_id`，按任务定义立即提交；停用定义409。 |
| `POST /v1/tasks/cancel` | body `task_id`；仅未开始任务，已执行/不允许状态409。 |
| `POST /v1/tasks/retry` | body `task_id`；仅 failed/dead，其他状态409。 |
| `GET /v1/dead-letters` | page/page_size；返回 list/page/page_size，无total。队列未创建或空队列返回200空列表；其他队列故障仍报错。 |

`params` 必须合法 JSON、不得显式 null，最大64KiB；timeout_secs 为0或1–86400，0采用服务默认。一次性提交/新建定义的 callback_url 非空400；更新定义只要传该字段即拒绝（包括空值）。地址由 `Config.CallbackTargetURL` 确定。旧 POST/PATCH 带业务ID路径不属于本表现行路由；没有 jobs DELETE。

`dept`、submitted_by 仅筛选/归因，不构成隔离策略；网关可把 submitted_by=me 转为真实操作者，taskrunner 不识别“me”的用户权限语义。

## 回调

`internal/callback/client.go` 的 `Client.Do` 对配置目标发 POST，body：

```json
{"task_id":"任务ID","request_id":"req-…","action":"audit_archive","params":{},"actor":"操作者","source_ip":"来源IP"}
```

actor/source_ip 为可选归因字段；action 是运行时标识，job 定义字段仍叫 action_id。body 为唯一业务负载通道，不将业务参数散到URL。回调带服务自身AK/SK签名；有request_id则头与body贯通。对端以task_id实现幂等，并在动作完全成功后返回2xx；4xx不可重试、5xx/超时可重试。任务级结果由查询API获取；可选终败通知是告警，不是第二套执行或结果复制通道。

## 源码定位与变更边界

- 受理与DB-first：`internal/service/submit.go` 的 `Service.Submit`。
- 查询/取消/重试：`internal/service/task_service.go` 的 `TaskService` 方法。
- 执行身份栅栏：`internal/worker/worker.go` 的 `Deps.handleCallback` 与 repository `MarkRunning` / `Finish`。
- notifier：`internal/notify/notifier.go`；部署值见 [部署](../ops/deployment.md)。

执行库的execution_id不进入本API。准确字段类型以请求结构、repository响应tag为准，新增接口须同步本页和调用方，不能用历史设计的设想代替实现。
