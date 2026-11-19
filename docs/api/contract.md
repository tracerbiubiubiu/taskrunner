# HTTP 与回调契约

本页描述当前实现，路由依据 `internal/handler/server.go` 的 `New`，绑定结构与查询解析也在该文件。业务状态依据 `internal/service/task_service.go`。源码整合与实际部署分别核对；后续合同见[平台集成](../design/platform-integration.md)，不能当作已有字段或路由。

## 接入与响应

业务路由为 `/v1`，仅内部网络并使用 utils `aksk.GinMiddleware` + `response.AKSKFail()`；探针 `/healthz`、`/readyz` 免鉴权且不返回内部依赖详情。响应采用 `{code,message,data,request_id}`，成功 code=0；错误保留 HTTP 状态。AK/SK 使用实际 pin 对应协议，query 不入签、时间窗不等于 nonce 单次消费。身份断言 `X-Operator` 和链路 `X-Request-ID` 纳入内部签名。

| 方法与路径 | 输入与输出 |
|---|---|
| `POST /v1/tasks` | `action` 必填；可选 `task_id`、`request_id`、`dept`、`params`、`submitted_by`、`source_ip`、`timeout_secs`。持久化成功即受理，Redis 暂不可用仍200待补投；不代表执行成功。相同 task_id、action及精确JSON语义params终身去重；同号异action/params返回HTTP409/code10005，零新增执行。 |
| `GET /v1/tasks/:id` | 按 task_id 查询现态，包含 dept 标签。 |
| `GET /v1/runs` | `id/request_id/action/status/job_id/dept/submitted_by/from/to/page/page_size`；dept 可重复多值，按运行快照列筛选，不 JOIN 当前 jobs；from/to 为 RFC3339。返回 list/total/page/page_size。 |
| `GET /v1/jobs/:job_id` | 经同一AK/SK验签读取单一定义，返回现有JobView（含真实action_id/params/enabled/updated_at）；不存在404。核心仅在已有职责允许的内部用例消费，不对前端直接开放。读取不是授权凭证，也不锁住后续更新。 |
| `GET /v1/jobs` | `dept/action_id/enabled/page/page_size`；返回分页任务定义。 |
| `POST /v1/jobs` | `action_id`、`trigger_type` 必填；可选 cron_spec/params/dept/enabled/timeout_secs/description/created_by/owner_service。 |
| `POST /v1/jobs/update` | body `job_id` 定位；可更新定义字段，省略表示保留。 |
| `POST /v1/jobs/trigger` | body `job_id`，按任务定义立即提交；停用定义409。可选预分配`task_id`和成对`expected_action/expected_params`，实际定义与期望精确不符409/code10005且零新run；未成对400。旧无新字段调用保持默认生成UUID。 |
| `POST /v1/tasks/cancel` | body `task_id`；仅当前未开始的执行，已执行或条件更新冲突返回409。队列前置查证不可用时，仅对仍未确认投递的 DB 行降级。DB 确认取消后返回成功，Redis 残留继续清理。 |
| `POST /v1/tasks/retry` | body `task_id`；仅 failed/dead，其他状态409；干预时队列已推进到pending/active也返回409，保留执行记录，新执行结果先落库时返回409。人工重试沿用队列已有重试计数和预算。真实Redis/未知错误仍500。 |
| `GET /v1/dead-letters` | page/page_size；返回 list/page/page_size，无total。队列未创建或空队列返回200空列表；其他队列故障仍报错。 |

`params` 必须合法 JSON、不得显式 null或包含重复对象键，最大64KiB，容器嵌套最多256层；省略沿原空对象缺省。action/params比对保留任意精度十进制数值，不经float64；键序、空白和1/1.0等数值等价不构成冲突。新提交的task_id最多64个Unicode字符，与核心VARCHAR(64)对齐，不按UTF-8字节截断；省略仍生成UUID，旧查询/重试标识不批量改写。timeout_secs 为0或1–86400，0采用服务默认。一次性提交/新建定义的 callback_url 非空400；更新定义只要传该字段即拒绝（包括空值）。地址由 `Config.CallbackTargetURL` 确定。旧 POST/PATCH 带业务ID路径不属于本表现行路由；没有 jobs DELETE。

`created_by/owner_service/submitted_by/actor`旧body字段兼容接收但不具有归因权威。经验签的caller与operator进入Service可信context；仅`security.trusted_operator_callers`列出的caller可声明非空真人operator（默认仅zhuzhao，必须有对应验签key，明确空列表表示不信任任何真人声明）。其它caller或无真人operator归因`service:<caller>`；无外部caller的内部调用归因`service:taskrunner`。owner_service取可信caller，更新保留原creator。该名单只影响归因，不能把任意签名operator当成核心授权。

`dept`、submitted_by 仅筛选/归因，不构成隔离策略；网关可把 submitted_by=me 转为真实操作者，taskrunner 不识别“me”的用户权限语义。

## 回调

`internal/callback/client.go` 的 `Client.Do` 对配置目标发 POST，body：

```json
{"task_id":"任务ID","job_id":"来源任务定义ID（有定义时）","request_id":"req-…","action":"audit_archive","params":{},"actor":"操作者","source_ip":"来源IP"}
```

actor/source_ip 为可选归因字段；可选job_id保留运行来源关联，一次性提交省略。所有字段均在签名body内；job_id本身不授业务权，核心固定job授权仍须受签查询真实task/job快照。action 是运行时标识，job 定义字段仍叫 action_id。body 为唯一业务负载通道，不将业务参数散到URL。回调带服务自身AK/SK签名；有request_id则头与body贯通。对端以task_id实现幂等，并在动作完全成功后返回2xx；408/429及5xx/超时可重试，其余4xx不可重试。只接受配置端点直接返回的完成信号，3xx保留为可重试的未确认结果且不跟随跳转；409仍表示快照冲突等终败，不用它承载在途状态。主仓在途/终态记账未确认的联合候选用503和Retry-After，只有动作已完成并确认终态才回2xx。任务级结果由查询API获取；可选终败通知是告警，不是第二套执行或结果复制通道。

这里的callback是**请求能力属主执行**，不是异步结果回贴入口；当前查询提供执行记录和状态，不代表已经包含业务结构化结果、工件授权或工单/AL字段回写。后续C03/D12将执行观察、属主效果receipt、结果投递和消费回贴分别核验，见[平台集成合同](../design/platform-integration.md#统一能力通道中的tr边界)。本表现有字段和线协议保持，不以新设计文字宣称新增结果API。

**完成信号与候选组合**：TR本候选在MarkRunning失败时不发回调，Finish失败时返回可重试错误，只有终态写入确认后才返回处理成功；delivery fence失效仍拒绝旧写者。本修复不增加业务receipt/unknown协议，真实跨仓完成语义还须绑定主仓503在途响应候选及联合验收；单独部署TR不能修复仍回2xx的旧主仓。

敏感人工Trigger由核心预分配task_id、先保存可信准入，再携带该ID及expected_action/expected_params调用。TR初读用于组装，`Store.ClaimJobRun`在pending提交的同一事务内CAS复验当前启用定义和完整执行快照，持锁后冻结载荷；若定义已变化则409且零新run。原task_id重复仍核action/params及job关联，不能借同号换job。该本地事务不取代核心最终执行资格，也不宣称跨库事务、完整receipt或unknown已实现。

## 源码定位与变更边界

- 受理与DB-first：`internal/service/submit.go` 的 `Service.Submit`。
- 查询/取消/重试：`internal/service/task_service.go` 的 `TaskService` 方法。
- 执行身份栅栏：`internal/worker/worker.go` 的 `Deps.handleCallback` 与 repository `MarkRunning` / `Finish`。
- notifier：`internal/notify/notifier.go`；部署值见 [部署](../ops/deployment.md)。

执行库的execution_id不进入本API。准确字段类型以请求结构、repository响应tag为准，新增接口须同步本页和调用方，不能用历史设计的设想代替实现。

## cron发生次与兼容

`Loop.FireDue`以原`jobs.next_run`时隙和job_id产生固定41字节`cron:<uuid>`任务ID；延迟tick/重启不改同一发生次身份。`Store.ClaimCronOccurrence`在一笔本地事务内CAS当前启用定义快照/原due，推进next_run并写pending/queued=0载荷；只有commit确认后才入队，入队失败交既有reclaim。两个共享同一事实库的loop只有一个赢得原时隙；推进或pending写失败整笔回滚，下轮重试原due。无有效未来时刻的cron不生成零时间发生次。

SQLite存量时刻可含Go monotonic后缀和命名时区，CAS使用同次读取的内部原始时间token，不用重新序列化的time.Time猜原文本；到期按解析后的绝对时刻过滤，避免跨时区文本比较误触发。新jobs时刻统一UTC并去monotonic，保留原宿主时区计算cron日历的政策；PG仍用原生timestamp比较。该兼容不重写存量数据或新增迁移。

不新增DDL/线协议发生次字段，不回写旧随机task_id；升级须停止旧scheduler和writer，按既有备份/排空规则切换，禁止新旧loop混跑。SQLite仍只支持单实例同一文件；多主机独立文件不共享事实源，本事务不承诺跨文件去重。共享PG的发生次CAS测试也不代表整服务HA已验。完整错失/恢复episode、长期委托、业务receipt与unknown仍归原C03/I03，核心本地归档和TR归档继续二选一。
