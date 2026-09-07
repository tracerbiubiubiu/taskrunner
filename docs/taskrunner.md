# taskrunner — 事件/任务总线（异步触发预置动作）

> 独立仓库、独立部署、独立 Redis、独立 DB。zhuzhao 的「异步执行侧」：zhuzhao 只作网关（鉴权/编排/业务审计），taskrunner 负责可靠调度、触发与执行回调，并提供内部 HTTP API（提交任务 / 查询执行记录 / 死信管理）。
> 设计 SSOT = 本文档；决策依据 = [ADR-002](./ADR-002-asynq-async-task-executor.md)（快照，zhuzhao 侧为 SSOT）。
> 2026-09-03 收敛 + 形态定稿；同日增补定稿：提交方式（调 API）、任务三层模型与定义 API 管理、HTTP API v1（含 jobs 组与结果查询语义）、job_runs 落库与两路日志分工、审计透传与归属过滤、asynqmon 库嵌入、实施计划 M1–M4、zhuzhao 侧配套清单。

---

## 1. 定位

**taskrunner = 事件/任务总线**：把"异步触发预置动作"做成独立能力。业务点在 zhuzhao 显式提交任务，taskrunner 保证任务**可靠执行**（不丢、可重试、可定时、可查），动作本身（业务 handler）仍在 zhuzhao 侧实现。

一句话：**zhuzhao 负责"决定要做什么"，taskrunner 负责"可靠地去做"。**

### 什么算一个「任务」（2026-09-04 确认）

**判定标准：任务 = 要求「必须发生、失败要重试、执行要留痕」的动作单位。** 满足越多越该走 taskrunner：

| 特征 | 走 taskrunner | 不走 |
|---|---|---|
| 调用方可立刻走人、不等结果 | ✅ 异步 | 要同步拿结果 → 普通接口调用 |
| 失败要自动重试 | ✅ 退避 + 死信 | 失败就算了 / 调用方自处理 |
| 要执行记录（何时/几次/成败） | ✅ job_runs | 不关心 |
| 定时 / 延迟触发 | ✅ cron | 即时即弃 |
| 动作耗时不稳定（网络 IO 如 SMTP） | ✅ 超时控制 | — |

- **示例——消息通知（ADR-002 场景 D，后续将集成）**：发消息就是普通预置动作。zhuzhao 注册 `notify_send` handler（发送要用 zhuzhao 的模板/用户偏好/notifications 表/SMTP 配置，按「函数访问什么数据」归 zhuzhao 侧）；业务点调 `POST /v1/tasks` **即时提交**（非 cron），taskrunner 秒级回调执行；taskrunner 侧零改动；
- **粒度**：默认一个动作单位一个任务；批量场景（一次命中 N 条）提交 1 个批量任务、handler 内循环处理，避免 N 次 HTTP 回调开销；
- **幂等提醒**：at-least-once 下重试可能重复执行（首次实际成功但响应超时），通知类一般可容忍，严格不重靠 zhuzhao 侧唯一键去重；
- **反面清单**：进程内轻量、丢了无所谓的动作（记缓存、打点）不过队列。

## 2. 与 zhuzhao 的关系

```
[用户] → zhuzhao（网关：鉴权/编排/业务审计）
              │ 调 taskrunner API 提交任务（✅ 定稿，见 §5）
              ▼
      taskrunner（独立仓库/部署/Redis/DB）
       ├── HTTP API server（任务定义管理 / 提交 / 查询 / 取消 / 重试 / 死信）
       ├── Asynq worker + cron 循环（分钟级 tick 扫 DB）
       ├── 消费任务 → HTTP 回调 zhuzhao 内网端点（携带 task_id + 参数 + request_id）
       ├── 失败重试 / 超时 / 死信
       └── 自维护 job_runs（独立 DB，执行记录，可经 API 查询）
```

- zhuzhao = 网关 + 业务归属（预置动作 handler 当前在 zhuzhao）；taskrunner = 通用调度/触发/重试运行时；（目标架构下 zhuzhao 薄化为 **API 网关 + IAM**，动作归属泛化为「能力属主服务」，见 §4 注记）；
- **不反向依赖**：taskrunner 不 import zhuzhao 代码，只通过 HTTP 回调执行预置动作、通过 API 接收任务；
- 任务提交方式 ✅ **定稿（2026-09-03）**：**调 taskrunner API 提交**（原 Open Question「直连 Redis Enqueue vs API」随「taskrunner 提供内部 HTTP API」一并落定；直连 Redis 不作为对外契约）。

### 独立部署定位与动作分发模式（2026-09-04 确认）

**定位**：taskrunner 定位为**可独立部署、可复用的通用调度平台**（独立仓库/部署/Redis/DB，§8）。「只做调度、不承载业务能力」是硬边界；与 zhuzhao 的关系是**协议级解耦**——taskrunner 只认 `action_id`（不透明字符串）+ `callback_url`（HTTP 端点），不感知对端是谁。

**当前与 zhuzhao 的耦合盘点（3 处，均可解耦，无需改代码）**：

| 层 | 实际依赖 | 解耦方式 |
|---|---|---|
| 编译期 | 仅依赖 zhuzhao-utils（`errcode`/`response`/`aksk`/`logger`），**不 import 任何 zhuzhao 业务代码** | 共享工具库不算业务耦合；彻底独立时把 `aksk` 等抽独立 module |
| 配置期 | `Security.Callers` 默认只配 zhuzhao 一个调用方（fail-closed：密钥环空拒绝启动）——本身是 **map**，可配多对端 | 改配置即可泛化，零代码改动 |
| 运行时 | 回调是纯 HTTP 协议（`CallbackURL` 由提交方指定；2xx=成功 / 4xx=不可重试 / 5xx+超时=重试 + AK/SK 验签） | 任何符合契约的服务都能当回调对端 |

**两种动作分发模式（判断）**：

- **模式 B · 能力集中（= 当前现状）**：所有动作集中在 zhuzhao `internal/jobs` 包（§4 三层模型「动作层」），taskrunner 只回调 zhuzhao。好处：样板（回调端点 + AK/SK 验签 + 幂等栅栏 + 状态语义）一次实现，接入方走 zhuzhao API 即用；代价：taskrunner 在某种意义上退化为 zhuzhao 的「执行组件」——脱离 zhuzhao 没有执行对象。适合「zhuzhao 是唯一消费者」的阶段。
- **模式 A · 能力分散（目标方向）**：taskrunner 为通用调度平台，各服务自己暴露回调端点（`/internal/actions/<id>` 类，契约同 §4）承接能力，zhuzhao 是第一个接入方。好处：调度器真正独立、可复用、多系统共用；代价：每个接入服务要复制「回调端点 + AK/SK 验签 + 幂等 + 状态语义」样板。

**结论**：两种模式的共同点「taskrunner 只做调度、数据不丢（`job_runs` 终身 + 幂等 + 重试 + 死信）」正确；**模式 A 更符合独立调度平台定位，且当前架构已天然支持 A**（协议级解耦，taskrunner 零改动）。当前阶段沿用模式 B（zhuzhao 唯一接入方）完全合理，**实现零改动**——不因「未来复用」提前重构。演进到 A 的触发信号与步骤：
1. **触发信号**：出现第二个需要接入调度的系统/服务；
2. **密钥环泛化**：`Security.Callers` 增配新调用方（已是 map，仅配置改动）；
3. **回调契约文档化**：把回调契约（body 字段 + 2xx/4xx/5xx 语义 + AK/SK 签名）固化为对端接入规范（当前摘要见 zhuzhao-integration.md §2.1）；
4. **可选**：taskrunner 内置动作注册表（同 §4「内置例外」：ping URL / 自清理等无对端动作进程内执行，不回调外部）。

## 3. 职责边界

**做**：
- 可靠任务队列（Asynq，Redis AOF 持久化，重启不丢）；
- 定时调度（**cron 循环**：分钟级 tick 扫 DB 到期定义，§10 定案——未用 asynq Scheduler，静态 payload 撑不起每次触发的新 task_id + 审计字段）；
- 触发 → HTTP 回调 zhuzhao 内网端点执行预置动作；
- 重试 / 超时 / 阻塞策略 / 死信；
- 自维护 `job_runs`（独立 DB：执行细节、重试次数、耗时、结果、回调目标，见 §6）；
- 内部 HTTP API（服务级鉴权）：提交、查询、取消/重试、死信管理、健康检查（见 §5）。

**不做**（2026-09-03 收敛）：
- ~~用户上传自定义脚本（python/shell）~~ → **🚦 降级**，按需再评估（选型参考 zhuzhao 仓库 `docs/phase3/15-script-platform-dagu-vs-inhouse.md`，见 §11）；
- ~~业务 handler~~ → 回调 zhuzhao，业务代码留在 zhuzhao；
- ~~事件源/事件流~~ → 事件事实源仍是 zhuzhao 的 L1 `ticket_events`（ADR-001），taskrunner 是执行侧不是事件源；
- 不替代任何消息中间件选型（Redis Pub/Sub 不持久化已排除、Kafka 对当前量级过度，见 ADR-001/002）。

## 4. 预置动作与任务定义（三层模型，2026-09-03 定稿）

"预置 task" 拆成三层，各归其位：

| 层 | 是什么 | 归属 | 变更方式 |
|---|---|---|---|
| **动作（action）** | 业务 handler，全局唯一 `action_id` | **能力属主服务**：内网端点 `/internal/jobs/<action_id>` + handler 注册表（当前 = zhuzhao；目标架构下 = 各业务服务，见下方「目标架构注记」） | 改代码发版（在属主服务） |
| **任务定义（job）** | `action_id + 触发方式（cron / 手动）+ params + enabled + 归属标签（如 dept，zhuzhao 写入）` | taskrunner **DB**（定稿：API 管理，不再用配置文件） | 运行时调 API（前端经网关操作） |
| **执行实例（run）** | 每次实际执行（含每次重试） | taskrunner `job_runs`（§6） | 自动产生 |

- 两层靠 **`action_id` 关联**：taskrunner 只认 id 和调度元数据，不感知函数；zhuzhao 侧用「接口 + 唯一 id 注册表」实现动作分发——

  ```
  // zhuzhao 侧（示意）
  type JobHandler interface{ Handle(ctx, params) error }
  var registry = map[action_id]JobHandler{ "audit_archive": …, … }
  // /internal/jobs/<action_id> → 查表分发执行
  ```

- **触发链路**：cron 到点 / `trigger` / `POST /v1/tasks` → taskrunner 按 job 定义组装回调（action_id + params + request_id）→ zhuzhao 查注册表执行 handler → 结果记 `job_runs`；
- **前端入口**（若有）：执行 / 加定时 / 启停的页面在 zhuzhao，链路 = 前端 → 网关（鉴权 + 业务审计）→ taskrunner API；cron 自动触发无用户参与，只记 `job_runs`；
- **调用人上下文透传（2026-09-03 补充）**：zhuzhao 调写接口（提交 / 建改定义 / 触发）时显式携带 `actor`（工号）与 `source_ip` 等原始信息，taskrunner **原样存档**（job 定义记 `created_by`；执行记录记 `submitted_by` / `source_ip`，cron 触发为空）并随 slog 打点（caller + actor + IP + request_id）——仅作审计归因，taskrunner 不校验、不据其做权限判断（信任边界在网关）；
- **归属过滤（2026-09-03 补充）**：job 定义的归属标签（如 `dept`）对 taskrunner 是不透明字符串，`GET /v1/jobs?dept=…` 按值过滤，实现「不同部门看到不同预置任务」；部门语义与「能看 / 能管哪些」的权限全归 zhuzhao（策略存 zhuzhao 自有 DB，见 §11 配套清单），taskrunner 不建组织模型；跨部门**写保护**随多调用方时代再启用（同 §5 `created_by` 口径）；
- **新增一个可调度动作**：① zhuzhao 新增 handler 并注册进注册表（发版一次）；② 调 `POST /v1/jobs` 建任务定义（运行时，无需再动 taskrunner）；
- 内置例外（后置，按需再启）：与业务无关的通用动作（如 ping URL、清理自身数据）可在 taskrunner 内实现同款「接口 + id 注册表」，进程内直接执行、不回调 zhuzhao；
- **能力服务（后置，2026-09-04 记录）**：动作按数据归属放——当前业务数据集中在 zhuzhao，所有动作集中在其 `internal/jobs` 包（加动作 = 加一个文件 + 注册一行，能力并不分散）。若出现以下信号，可新建专门的能力服务承接动作，**模型完全兼容、taskrunner 零改动**（它只认 `action_id + callback_url`，端点在哪不感知）：① 通用动作（不属于任何业务域的 ping / 搬运 / 报表类）批量涌现；② 多个服务都需要注册动作、分散管理成为负担。触发前不预建（新服务承接业务动作要么跨服务连库、要么逻辑劈两半，均为负收益）。
- **目标架构注记（2026-09-07）**：zhuzhao 演进方向 = **API 网关 + IAM**（薄网关：鉴权、代理任务提交/查询，不持有业务能力），业务数据与能力下沉各服务。动作归属随之泛化为「能力属主服务」：各服务挂自己的动作端点，taskrunner 统一调度全部服务的动作（xxl-job「一调度中心 + N 执行器」形态）——模型零改动，只认 `action_id + callback_url`；下文「能力目录」即为此铺路（端点自注册、提交方不再手填地址）。handler 随数据迁移，taskrunner 仅改路由指向。多服务时代启用已预留的 `owner_service` 与多调用方 credential（§5）。「一处添加所有能力」仅对通用动作成立（→ 能力服务后置项，上一条）；服务专属动作加在属主服务，是「各服务的能力在各服务里」的直接推论。

### 回调契约（2026-09-03 补充）：
- **幂等**：任务重试可能重复回调，zhuzhao handler 须按 `request_id`/`task_id` 幂等（先查重再执行）；对「导出 + 删除」类有副作用的动作尤其必要；
- **语义**：at-least-once——幂等是 zhuzhao 侧义务，不是 taskrunner 承诺；
- **结果归属**：回调的 HTTP 响应即本次执行结果，handler 执行侧当场感知成败（仅此一次同步感知）；对提交方**不推送**，任务级执行结果一律经 §5 查询接口获取，详细过程只在 taskrunner `job_runs`，不回传、不复制（§7）；
- **终败通知（后置，按需启用）**：默认不做任何结果推送——执行结果一律由 zhuzhao 按需查询（§5）；死信的知情路径默认是 M4 死信告警（运维侧）与 `/monitor` 看板。仅当出现「无人盯守的周期任务、失败需业务方主动知情」的场景时，再启用死信回调通知（如 `POST /internal/notifications/task-dead`，带 task_id / request_id / 最后错误）；
- **重试判定**：5xx / 超时 → 按 Asynq 退避策略重试；4xx → 不重试，直接判失败；HTTP 2xx = **执行完全成功**（✅ 拍板定案 2026-09-03：~~业务级失败用响应体状态字段表达~~ **无状态字段**——zhuzhao handler 直接用 HTTP 状态码表达业务失败：不可重试 → 4xx、可重试 → 5xx，走常规 errcode 映射；taskrunner 侧现有 2xx/4xx/5xx 判定即最终行为）；
- **超时**：默认值落地时定（建议 30s），可按 action 覆盖；
- **L1 边界**：回调执行产生的业务事件（如 SLA 违约）仍由 zhuzhao 侧 handler 落 L1 `ticket_events`；taskrunner 只触发回调、**不写业务事实**；
- **安全边界**：独立部署下需明确内网可达性（如同 VPC）。~~回调鉴权~~ ✅ **基线修订定案（2026-09-03，覆盖当日早前「不做独立回调鉴权」拍板）**：回调请求带 **AK/SK HMAC 签名**（C9：callback client 以 taskrunner 自身 SK 签名，覆盖 X-Request-ID / X-Operator / body；zhuzhao `/internal` 端点验签）；~~capability URL 增强~~ 随之作废；专用 network 为第二道防线。
- **消息体约定（2026-09-04 补充）**：业务参数（`params`）**统一走回调请求 body**——唯一业务负载通道、任意 JSON、进 AK/SK 签名覆盖范围；各字段各归其位，业务参数不散落到 query / header / 路径：
  - 路由标识 `action` → URL 路径 `/internal/jobs/:action_id`；
  - 链路标识 `request_id` → Header `X-Request-ID`（body 内冗余携带兜底，与 header 一致）；
  - 鉴权 → 签名层（AK/SK HMAC 覆盖 body + 关键头）；
  - **业务参数 `params` → Body（唯一业务通道）**。
- **统一回调 body schema（执行端 SDK 的入口约定）**：`{ "task_id": …, "request_id": …, "params": … }`——执行端 SDK 解包统一 schema → 校验 → 取 `params` 传 `jobs.Registry` Handler；**加能力 = 注册 code + 写 Handler，回调入口零改动**；幂等（task_id）、链路（request_id）、错误映射（2xx/4xx/5xx）在 SDK 层统一处理，业务 handler 只关心 `params` 与返回值；
- **params 存储与传输分离**：存储 = 入队时 Asynq payload + 执行记录 job_runs（taskrunner 内部持久化）；传输 = 回调 body（给执行端的唯一业务通道）——两者不混。

### 能力目录（capability_registry）【方案待定 · 2026-09-04 记录，未拍板】

**现状**：任务定义（job）携带**提交方指定**的 `callback_url`，taskrunner 直接回调——「能力 code → 服务端点」的路由由提交方手填，无跨服务能力发现。

**方案（待定）**：执行端把「能力唯一 code + 对应路由（callback_url + auth_key）」注册进 taskrunner DB（`capability_registry`），taskrunner 提交任务只认 `action_code`，自行查表解析回调目标。

- 表（taskrunner 侧 `internal/repository`，同 job_runs 领域 schema 口径，不放 zhuzhao-utils）：`code` PK / `callback_url` / `auth_key`（指向密钥环条目）/ `enabled` / `created_at` / `updated_at`；
- **注册**：执行端服务启动自注册 `POST /internal/actions`（upsert 幂等）+ 管理 / 配置兜底（避免启动顺序依赖）——能力服务（network 等）上报后即被 taskrunner 发现，提交方无需知道实现地址；
- **提交**：job 定义不再必填 `callback_url`，只填 `action_code`；提交时 taskrunner 查表解析出 `callback_url` + `auth_key` **快照入 job 行**（运行期不依赖目录在线，故障隔离、可追溯「当时调用了谁」）；显式 `callback_url` 保留覆盖能力（调试 / 直连 / 未注册目标）；
- **回调**：复用现有回调 client，按 job 行快照的 URL + auth_key 签名回调（AK/SK HMAC，§4 回调契约安全边界）；
- **两级路由**：跨服务目录（code → 服务端点，taskrunner 查）+ 服务内 `jobs.Registry`（code → Handler，执行端查）——各管一层、不重复；与 §4「能力服务（后置）」条目衔接（端点从"提交方手填"演进为"能力服务自注册"，taskrunner 仍只认 code）；
- **规模控制**：单表 + upsert + 提交时解析，不引入 etcd / consul 注册中心（量级上来再评估）。

**实现位置（2026-09-04 补充，随方案待定）**——归属原则同 §6 job_runs：taskrunner 领域 schema / 流程放 taskrunner 仓库，多个执行端复用的通用件放 zhuzhao-utils：
- `capability_registry` 表 → taskrunner `internal/repository`（领域 schema，唯一查询方 = taskrunner，**不放 zhuzhao-utils**）；
- 注册 API（`POST /internal/actions`）→ taskrunner `internal/handler`（upsert 幂等；管理 / 配置兜底同层）；
- 提交时解析快照 → taskrunner `internal/service`（submit 流程内查表 → 快照 `callback_url` + `auth_key` 入 job 行）；
- 执行端「启动自注册」client → zhuzhao-utils（执行端 SDK 内，多执行端复用，调注册 API + AK/SK 签名）；
- 可选 `RegistryClient` 接口契约（Register / Resolve / Heartbeat）→ zhuzhao-utils 放**接口定义**，DB 实现在 taskrunner——为将来换 etcd / k8s 实现留平滑迁移口（业务零改动，实现方只换实现不换契约）。

⚠️ **方案待定**（2026-09-04，机制方向已确认有价值，具体点未拍板、待讨论）：
- 注册表归属：taskrunner DB（唯一查询方，倾向）vs 独立注册中心；
- 注册方式：自注册 vs 配置录入 vs 两者并存；
- 解析时机：提交时快照（倾向）vs 每次回调实时查；
- 显式 `callback_url` 覆盖是否保留；
- 与「密钥环多对端」合并为同一张表，还是分表。

落地前需定稿。

## 5. HTTP API（内部，v1 范围定稿）

taskrunner 形态 = **Asynq worker + 常驻 HTTP server**（同进程部署，拆分留待需要时再议）。

- 暴露边界：**仅内网 + AK/SK HMAC 签名**（✅ 基线修订拍板 2026-09-03：服务间通信一律签名/验签——utils `aksk` 包，按调用方发 SK；**覆盖当日早前「零认证+拓扑」与 M2 静态 Bearer 两条口径**：C2 从「拆 Bearer」改为「Bearer → AK/SK 验签」，不再依赖拓扑时序；专用 network 保留为第二道防线）。基线 SSOT = zhuzhao `docs/phase3/16-external-integration.md` §9。分工：**用户权限（zhuzhao 三层校验）全部收敛在网关**——前端所有操作先过网关授权，通过后才代理调 taskrunner API；taskrunner 不认证用户、不复刻权限模型；
- 调用方身份：API 写操作（建/改定义、触发、取消、重试）在 slog 打点归因（caller + actor + IP + request_id——**B2 缺口，随 §10 C1 统一访问日志中间件补齐**）；`job` 表记 `created_by`（工号，§4 透传，审计归因）与 `owner_service`（当前恒为 zhuzhao）——跨服务归属约束待第二个调用方出现再启用，当前不实现；
- 响应结构 / 错误码复用 zhuzhao-utils `errcode` + `response`，与网关风格一致。

| 端点 | 用途 |
|---|---|
| `POST /v1/tasks` | 提交任务；**受理语义**：校验通过 + 写入队列（Redis AOF）即返回 task_id，「受理 ≠ 执行成功」；**幂等**：接受调用方生成的 `task_id`，重复提交去重 |
| `GET /v1/tasks/{id}` | 查任务状态 |
| `GET /v1/runs?request_id=&action=&status=&job_id=&from=&to=` | 查执行记录（§7 日志边界的「request_id 跨查」即此；job_id 过滤按定义关联） |
| `GET /v1/jobs?dept=&action_id=&enabled=` / `POST /v1/jobs` | 列出（支持按归属标签等过滤）/ 新增任务定义（action_id + cron 或手动 + params + enabled + 归属标签，§4） |
| `PATCH /v1/jobs/{id}` | 修改任务定义：cron / params / 启停 |
| `POST /v1/jobs/{id}/trigger` | 手动执行一次（按定义提交任务，前端「立即执行」按钮） |
| `POST /v1/tasks/{id}/cancel` | 取消未开始的任务 |
| `POST /v1/tasks/{id}/retry` | 重试失败/死信任务 |
| `GET /v1/dead-letters` | 死信列表（配合 Asynq Inspector 重放，死信处理闭环） |
| `GET /healthz` | 健康检查（存活） |
| `GET /readyz` | 就绪探针（检 Redis ping + SQLite；C4） |

**结果获取模型（2026-09-03 定稿）**：提交响应只回答「是否受理」；执行是否成功，一律通过 `GET /v1/tasks/{id}` / `GET /v1/runs` **按需查询**——查询是执行结果的唯一出口，taskrunner 不主动推送（终败通知为后置项，§4）。

**明确后置**（不算缺口）：全文检索、聚合统计、深分页——当前无日志平台，临时查库；未来上 ES 等平台时归平台（演进路径见 §6）。

## 6. job_runs 存储与日志分工（2026-09-03 定稿）

**执行记录落库**（不存文件、不依赖 Asynq/Redis 自带记录——Redis 只保队列运转，撑不起按 request_id/action/时间段的查询）：

- 存储：**独立 DB**。~~SQLite 起步~~ ✅ **拍板统一 PG（2026-09-03）**：迁独立 PG 数据库（utils `postgres` + pgx，schema 不变，随 M3/M4 C7 落地）；SQLite 保留为 M1/M2 已交付实现（`database/sql` 接口无感切换）；
- 最小 schema：`task_id`、`request_id`、`action`、`status`（pending / running / succeeded / failed / dead）、`attempts`、`callback_url`、`error`、`duration_ms`、`submitted_by` / `source_ip`（zhuzhao 透传的原始调用人，cron 触发为空，仅审计归因）、`enqueued_at` / `started_at` / `finished_at`；
- 存储层代码放**本仓库 `internal/repository`**（2026-09-04 结构重构，原 `internal/store`）：job_runs 是 taskrunner 领域 schema，**不放 zhuzhao-utils**（utils 只收通用件；出现第二个同类消费者再考虑下沉）；
- 保留策略 ⚠️ 落地时定（建议：保留期可配置 + 定时清理，思路同审计归档；`submitted_by` / `source_ip` 属个人信息，同受保留期约束）。

**两路日志分工**（"日志"是两件事）：

| 路 | 内容 | 载体 | 消费方式 |
|---|---|---|---|
| 执行事实 | task_id / request_id / action / 状态 / 重试 / 耗时 / 结果摘要 | `job_runs` 表 | 查询接口（§5） |
| 过程细节 | 回调请求/响应报文、堆栈、debug 输出 | 应用日志文件（zhuzhao-utils `logger`：slog JSON + lumberjack 轮转） | 人工排障（grep 文件）；后续采集入 ES（见下） |

- 关联约定：任务执行链路统一 `slog.With("request_id", …, "run_id", …)` 打点——表里查到一条记录，拿 id 即可在日志文件中定位完整细节；
- **ES 演进路径（2026-09-03 确认：当前无日志平台，先落文件）**：过程日志即 slog **JSON Lines**——一行一条结构化记录，字段即索引结构，上 ES 时只需加 Filebeat / Vector 等 shipper tail 日志文件（处理轮转）→ ES / Loki，**taskrunner 应用代码零改动**；前提是打点字段保持稳定命名（`request_id` / `run_id` / `action` / `task_id`），M1 起即按此约束写打点。

## 7. 日志边界（2026-09-03 确认）

| 层 | 记什么 | 归属 |
|---|---|---|
| 业务审计 | 用户/网关侧业务操作 | zhuzhao `audit_logs` |
| 任务提交日志 | `{ action, task_id, request_id }` | zhuzhao（提交凭证，薄） |
| 任务运行日志 | 执行细节/重试/耗时/结果 | taskrunner 自维护 `job_runs`（独立 DB，见 §6），**不传回 zhuzhao** |

- 两边以 **`request_id` 关联**（zhuzhao 提交任务时生成/透传，taskrunner 在 job_runs 记下）；
- 需要追溯时跨查：zhuzhao 以 request_id 调 taskrunner `GET /v1/runs?request_id=…`（§5），**不复制**对方数据；
- 跨查接口只暴露内网，用户侧查询经 zhuzhao 网关代理。

## 8. 部署形态（2026-09-03 定稿）

- **独立仓库**（本仓库，可独立部署）；
- **独立部署**（独立进程/容器，不与 zhuzhao 布一块——zhuzhao 只作网关调用各能力、拉起各任务）；
- **独立 Redis**（Asynq 队列归属 taskrunner，能力自包含，同 activelist 独立库原则）。口径：指 Redis 命名空间/库独立归属 taskrunner，**不必然新增一套 Redis 部署**，与 ADR-002「复用现有 Redis、不新增基础设施」不冲突，按部署环境落地；
- **独立 DB**（`job_runs` 归 taskrunner 自有）。~~SQLite 起步~~ ✅ **拍板统一 PG（2026-09-03，基线 §8/zhuzhao 16 号 §9 C7）**：迁独立 PG 数据库（复用 utils `postgres`，schema 不变，约半天，随 M3/M4）；SQLite 保留为 M1/M2 已交付里程碑实现；
- **Docker 部署（2026-09-03 确认）**：单容器单进程（API server + Asynq worker + cron 循环同进程，§5）；挂载卷：日志目录（轮转文件持久在宿主，供排障与后续采集）；配置走环境变量（C6 迁 yaml+`${VAR}`）。~~SQLite 单写者 → 单副本部署~~（PG 后解除，多副本按运维需要）；`/monitor` 随容器同端口暴露，仅内网可达；
- 公共工具统一引自 [zhuzhao-utils](https://github.com/tracerbiubiubiu/zhuzhao-utils)：`logger`（应用日志）、`postgres`（迁 PG 时）、`errcode` + `response`（API 统一响应）；`redis` 包用不上（Asynq 走自己的 `RedisClientOpt`）。依赖 utils（独立通用工具库）**不属于**「不反向依赖」的禁止范围。

### 工程结构与依赖注入（Wire，2026-09-04 确认）

以正式微服务标准建设，内部与 zhuzhao 同规格（对齐 zhuzhao 16 号 §9「工程结构」基线）——**Wire DI + 分层**，入口薄、装配集中在 `internal/app`。

**目录分层**：

| 目录 | 职责 |
|---|---|
| `cmd/taskrunner` | 薄入口：`serve`（常驻服务，默认 `configs/config.yaml`，纯 env 亦可）/ `enqueue`（CLI 调试入队，**手工装配不走 Wire**） |
| `internal/app` | **装配与生命周期**：`wire.go`（Wire 注入描述）/ `wire_gen.go`（生成物）/ `providers.go`（12 个 provider 构造函数）/ `app.go`（Run 优雅启停） |
| `internal/config` | 配置加载（C6：yaml + `${VAR}` 展开 + 全 env 兼容；密钥环空 / self_sk 缺失 fail-closed） |
| `internal/handler` | 薄 HTTP 层：绑定/映射 + service 错误→HTTP 映射；业务在 service |
| `internal/service` | 业务下沉：`TaskService`（提交/查询/取消/重试/jobs 定义）+ `submit`（统一受理，终身幂等）+ `cron`（分钟级 tick 扫 DB） |
| `internal/repository` | job_runs / jobs 持久化（`database/sql`，SQLite 起步 → PG 无感切换） |
| `internal/middleware` | C1 访问日志 + C2 AK/SK 验签 |
| `internal/worker` | Asynq worker：统一 `taskrunner:callback` 处理器（终态判定） |
| `internal/callback` | 回调客户端（C9：自身 SK 签名 + rid 透传 + 2xx/4xx/5xx 判定） |
| `internal/task` | Asynq 载荷类型（跨进程契约，字段命名保持稳定） |

**Wire 装配**（`internal/app`）：
- `wire.go`（`//go:build wireinject`）声明注入集合：`wire.Build(NewApp, provideLogger, provideStore, provideRedisOpt, provideSubmitter, provideInspector, provideTaskService, provideCron, provideAsynqServer, provideCallback, provideKeys, provideReadyz, provideEngine)`；`wire_gen.go`（`//go:build !wireinject`）为生成物——**生成器依赖暂未入 go.sum，改动 `wire.go` 后按 Wire 语义手工同步 `wire_gen.go`**（与 zhuzhao wire_gen 风格对齐）；
- `providers.go` 提供全部构造函数，依赖由 Wire 解析——显式依赖图 + **编译期校验**（provider 缺失 / 类型不匹配编译即失败，非运行时 panic）；
- 装配期单例：Store / Redis client / Inspector / readyz-redis 各建一次；`InitializeApp` 统一返回 cleanup（client / inspector / readyz-redis Close、store Close）。

**生命周期**（`app.go Run`）：`serve` → `config.Load` → `InitializeApp(cfg)` → `a.Run()`——HTTP server（`/healthz` `/readyz` `/v1`）+ Asynq worker + cron loop **同进程并发**（§5）；收到 SIGINT/SIGTERM → HTTP drain（10s 超时）→ worker 收尾（等在跑任务完成），干净退出。

**为什么用 Wire**：依赖图显式可见、装配错误编译期暴露（vs 手写 init 的运行时 panic）、cleanup 集中管理（配合容器优雅停机）；与 zhuzhao 工程基线一致，降低跨仓库认知成本。

## 9. 首个预置动作（M-E 验收入口）

> 代号说明：B11② = zhuzhao 排期中的功能项编号；M-E = 里程碑验收入口。出处见 §11 关联文档。

- **审计归档（B11②）**：定时（如每日）回调 zhuzhao，导出 `audit_logs` 超期数据为 JSONL 并删除（保留期可配置）；
- 退出标准：归档任务按周期跑通；预置动作「触发 → 回调执行 → 失败重试」闭环；关联验收见 zhuzhao 仓库 `docs/phase3/03-audit-l2.md`（B11② 设计，见 §11 关联文档）。

## 10. 实施计划（2026-09-03 整理）

整体排期 SSOT 仍是 zhuzhao `docs/phase3/13-implementation-plan.md`（M-E 行）；本表只定**模块内顺序**。

| 里程碑 | 内容 | 出口标准 |
|---|---|---|
| **M1 核心运行时** | Asynq worker；回调客户端（超时 / 5xx·4xx 判定 / 退避重试）；`job_runs` 落库（SQLite + `internal/repository`）；`/healthz`；入队暂以 CLI / 测试入口触发 | 任务能从入队走到回调并正确记录，失败按策略重试，死信可查 |
| **M2 HTTP API** | §5 全部 v1 端点（含任务定义 jobs 组、归属标签过滤）；内网 credential 鉴权；调用人上下文字段（`actor` / `source_ip`）；`errcode`/`response` 统一响应；提交幂等（task_id 去重） | zhuzhao 可走 API 建任务定义、提交任务，并按 request_id 查到执行记录 |
| **M3 首个预置动作** | 审计归档（B11②）：创建任务定义（job）+ zhuzhao `/internal/jobs/<action>` 端点 | 「触发 → 回调执行 → 失败重试」按周期闭环跑通（对齐 zhuzhao `docs/phase3/03-audit-l2.md`） |
| **M4 运维完善** | 指标（队列深度/成功率/回调延迟）；死信告警；asynqmon **以库嵌入 API server**（挂 `/monitor`，置于内网 token 之后，read-only 起步，前端随包内嵌；入队配 `asynq.Retention` 短期留观——Redis 里那份只作近期观察，长期事实以 `job_runs` 为准）；`job_runs` 保留清理 | 异常可感知、死信有出口、存储有界；运维看板可用且不裸暴露 |

模块内顺序 **M1 → M2 → M3**，M4 可与 M3 并行。

⚠️ 随 M2/M3 落地细化（M2 已定案部分随实现入档）：
- ~~动态 cron 实现方式~~ ✅ M2 定案：**分钟级 tick 扫 DB**（`cronloop`，默认 30s 轮询）——定义是 DB 数据、增改停启下个 tick 生效；宕机错失的触发重启后至多补一次。未选 asynq Scheduler 热重注册：静态 payload 撑不起「每次触发生成新 task_id + 审计字段」；
- ~~credential 具体形式~~ ✅ M2 定案：静态 Bearer token（`TASKRUNNER_API_TOKEN`）——**2026-09-03 被 AK/SK 基线修订覆盖：Bearer → AK/SK HMAC 验签**（utils `aksk`，C2/C8）；
- **公共能力对齐清单（zhuzhao 16 号 §9 C1–C9；2026-09-04 结构重构批量收口）**：~~C1~~ ✅ 统一访问日志中间件（rid 读头/回显 + X-Operator 兜底 `system`）；~~C2~~ ✅ API 验签 AK/SK HMAC（Bearer 已移除，密钥环空拒绝启动）；~~C4~~ ✅ `/readyz`（Redis ping + SQLite 探针）；~~C5~~ ✅ Dockerfile `TZ=Asia/Shanghai`；~~C6~~ ✅ viper yaml+env（TASKRUNNER_* 全兼容）；~~C9~~ ✅ 回调以自身 SK 签名 + rid 透传；**C3**（compose 双 network）与 **C7**（SQLite→PG）随部署批次；
- ~~回调鉴权机制（taskrunner → zhuzhao `/internal`）~~ ✅ 拍板定案（2026-09-03）：不做独立机制——信任边界 = 内网隔离 + `callback_url` 由 zhuzhao 提交时指定；**2026-09-03 被 AK/SK 基线修订覆盖**：回调请求带 HMAC 签名（§4 安全边界，C9，zhuzhao `/internal` 验签），capability URL 增强作废，专用 network 降为第二道防线；
- ~~`GET /v1/tasks/{id}` 状态数据源~~ ✅ M2 定案：**job_runs 为主 + Asynq Inspector 补充 in-flight 实时态**（pending/active/retry/scheduled 只在 Redis，以 `live_state` 字段并返回）；
- ~~action 校验方式~~ ✅ M2 定案：**不做前置校验**——不存在 / 未注册的 action 经回调 4xx 快速失败（non-retryable，failed 可见）；zhuzhao 清单端点方案保留为可选增强；
- 回调超时默认值：实现取 30s（env `TASKRUNNER_CALLBACK_TIMEOUT` 可改，载荷可按任务覆盖）——随 M3 验证后转正式口径；
- **同一 job 重叠执行策略（2026-09-03 登记，随 M3/M4 落地）**：现状 cron 到点即触发新 task_id，上次未完成也再触发一份（重叠允许，仅幂等兜底）。job 定义缺 overlap 策略字段——建议增 `overlap_policy: allow | skip_if_running`（默认 allow；audit_archive 等周期批任务配 skip_if_running，对齐 zhuzhao 13 号 M-E「阻塞策略按任务拍板」）；
- `job_runs` 保留期：仍待定（M4 随清理任务落地）。

## 11. 关联文档

| 文档 | 位置 | 关系 |
|---|---|---|
| [ADR-002](./ADR-002-asynq-async-task-executor.md) | 本仓库 docs/（快照） | Asynq 执行器决策，zhuzhao 侧为 SSOT |
| [zhuzhao-utils](https://github.com/tracerbiubiubiu/zhuzhao-utils) | 独立仓库 | 通用工具库（logger / postgres / errcode+response），非反向依赖 |
| zhuzhao `docs/adr/ADR-001-event-mechanism-l1-steady-state.md` | zhuzhao 仓库 | L1 事件源（taskrunner 不替代） |
| zhuzhao `docs/phase3/13-implementation-plan.md` | zhuzhao 仓库 | 排期 SSOT（M-E 行 + 部署注记） |
| zhuzhao `docs/phase3/14-planning-overview.md` | zhuzhao 仓库 | 规划视图 |
| zhuzhao `docs/phase3/15-script-platform-dagu-vs-inhouse.md` | zhuzhao 仓库 | 脚本任务选型调研（🚦 按需再启参考） |
| zhuzhao `docs/phase3/03-audit-l2.md` | zhuzhao 仓库 | 审计归档（B11②）设计 |
| [zhuzhao-integration.md](./zhuzhao-integration.md) | 本仓库 docs/ | **zhuzhao 侧配套需求清单**（六项展开 + 契约摘要，见下） |

### zhuzhao 侧配套需求（跨仓库依赖，勿遗漏）

本设计引出的 zhuzhao 侧工作项已**拆出独立文档**：[zhuzhao-integration.md](./zhuzhao-integration.md)（动作注册表与内网端点、动作清单校验端点、任务管理功能、部门可见性策略及其存储、任务提交日志、后置的通知接收端点，含交互总览与回调契约摘要）。设计与排期以 zhuzhao 仓库为 SSOT，该文档供 zhuzhao 侧排期与实现参考。

## 12. 状态与变更记录

| 日期 | 变更 |
|---|---|
| 2026-09-03 | 建仓 + 建档：从 zhuzhao 13/14/15/ADR-002 提炼本设计文档；形态定稿（独立仓库/部署/Redis + 回调模型 + 日志边界 request_id 关联） |
| 2026-09-03 | 修订：明确独立 Redis 口径；补充回调幂等 / L1 边界 / 安全边界；job_runs 存储注记；提交方式标为 Open Question；代号说明；README 状态表述修正 |
| 2026-09-03 | 设计增补（运行时与契约）：提交方式定稿「调 taskrunner API」（关闭 Open Question）；新增 §5 HTTP API v1（端点集 + 内网鉴权边界 + 结果获取模型「受理 vs 查询唯一出口，不主动推送」）；§6 job_runs 落库（独立 DB / SQLite 起步 / `internal/store`，取代「默认随独立 Redis 落盘」注记）+ 两路日志分工（表存事实 / 文件存细节 / slog 关联）；§4 回调契约补全（at-least-once / 重试判定 / 超时 / 响应约定；终败通知曾提议后列为后置）；§10 实施计划 M1–M4 |
| 2026-09-03 | 设计增补（模型与治理）：§4 三层模型（动作 action / 任务定义 job / 执行实例 run），job 管理定稿「存 taskrunner DB + API 管理」（关闭「配置文件 vs API」待定项）；角色分工澄清——发起 / 下发 cron / 查日志由 zhuzhao 发起，调度·触发·重试·记录归 taskrunner，zhuzhao 在执行环节仅被动接收回调；审计透传（`actor` 工号 / `source_ip`）与归属标签过滤（`dept`，部门可见性策略存 zhuzhao 侧）；§5 调用方身份（per-caller credential + slog 归因，`created_by` / `owner_service` 字段）；§8 部署形态补独立 DB 与 zhuzhao-utils 依赖；M4 定稿 asynqmon 以库嵌入 `/monitor`（read-only 起步） |
| 2026-09-03 | 补充 §11 zhuzhao 侧配套清单（动作注册表与内网端点、动作清单校验端点、任务管理功能、部门可见性策略及其在 zhuzhao DB 的存储、任务提交日志、后置的通知接收端点），防跨仓库工作项遗漏 |
| 2026-09-03 | 全文整理：修正 ADR-002 注记章节引用（§6→§8）；统一 `created_by` / `owner_service` 字段口径；对齐「结果归属」与「结果获取模型」表述；M3 去除 `job_config` 旧术语；M1/M2 补触发方式与透传字段；待细化项集中至 §10 ⚠️；变更记录按主题合并 |
| 2026-09-03 | 环境决策与配套拆分：确认无日志平台（过程日志先落文件，§6 入档 ES 演进路径——slog JSON Lines，shipper 采集零改应用，字段稳定命名自 M1 约束）；部署形态定 **Docker**（单容器单进程、卷挂载 SQLite/日志、SQLite 单写者 → 单副本约束入档）；zhuzhao 侧配套需求拆出独立文档 [zhuzhao-integration.md](./zhuzhao-integration.md)，§11 改为链接；M4 补 `asynq.Retention` 留观口径 |
| 2026-09-03 | M1 完成（feat/m1-runtime）：核心运行时落地——Asynq worker + 回调客户端（2xx/4xx/5xx·超时判定）+ job_runs SQLite（WAL）+ healthz + enqueue CLI + Dockerfile；端到端冒烟通过（成功 / 4xx 死信 / 幂等重提）；实现中修正：store 自动建目录、先入队后落库防孤儿行 |
| 2026-09-03 | M2 完成（feat/m2-api）：HTTP API v1 全量落地——gin + utils `errcode`/`response` 统一响应；静态 Bearer 鉴权（未设 token 拒绝启动）；提交 / 查询（runs 条件分页 + live_state）/ jobs CRUD（dept 过滤、cron 校验）/ trigger / cancel / retry / 死信列表；`cronloop` 分钟级 tick 触发 cron 定义（§10 定案入档）；job_runs 增 `job_id` 列关联任务定义；API 层选型：token 取消用 asynq Scheduler 改自研 tick（静态 payload 限制）；单测 19 个 + 真 Redis 端到端冒烟（含 cron 到点自动触发、停用即不触发）|
| 2026-09-04 | 代码评审修复（fix/idempotency-and-cancel-race）：**提交幂等扩为终身**——原仅靠 Asynq TaskID 冲突（succeeded 后 ID 释放，同 task_id 重提会重复执行并覆盖历史），Submit 入口先查 job_runs、行存在即幂等拒绝；**cancel 竞态三道防护**——删队列前 GetTaskInfo 查 active、DeleteTask 报 active 映射 409、MarkCanceled 改条件更新（WHERE status=pending）|
| 2026-09-04 | §1 新增「什么算一个任务」判定标准（任务 = 必须发生/失败重试/执行留痕的动作单位；含消息通知示例——ADR-002 场景 D 即普通预置动作、即时提交非 cron、taskrunner 侧零改动；粒度与幂等提醒；反面清单）|
| 2026-09-04 | 澄清任务完成情况查询口径（zhuzhao-integration §2.3 补充）：业务查询走 `/v1/runs?status=…`（job_runs 为数据源），asynqmon 仅为 M4 运维看板（队列内部态、无业务过滤）不面向用户查询；可选增强：状态计数聚合端点，做页面时按需 |
| 2026-09-04 | §4 新增后置项「能力服务」：动作按数据归属集中放（当前 = zhuzhao `internal/jobs` 包，加动作一处完成）；通用动作批量涌现或多服务注册动作成负担时，可新建能力服务承接——模型兼容、taskrunner 零改动，触发前不预建 |
| 2026-09-04 | **工程结构与依赖注入补档**（§8 新增子节）：正文补 Wire DI 说明——目录分层表（cmd 薄入口 / internal/app 装配 / handler / service / repository / middleware / worker / callback / task）、Wire 装配（wire.go 注入集合 + wire_gen.go 生成物 + 手工同步约定 + 编译期校验 + cleanup）、生命周期（HTTP + worker + cron 同进程、SIGTERM 优雅停止）、为什么用 Wire（依赖图显式/编译期暴露/cleanup 集中，对齐 zhuzhao 工程基线） |
| 2026-09-04 | **独立部署定位与动作分发模式确认**（§2 新增子节）：taskrunner = 可独立部署的通用调度平台，与 zhuzhao 协议级解耦——耦合仅 3 处（编译期依赖共享 utils、配置期 Callers 默认单对端、运行时 HTTP 回调协议），均可解耦且**实现零改动**；对比「模式 B 能力集中（现状）vs 模式 A 能力分散（目标）」，确认 A 更符合独立调度定位且当前架构已天然支持，演进触发信号 = 出现第二个接入系统（届时密钥环泛化 + 回调契约文档化 + 可选内置动作注册表，不提前重构） |
| 2026-09-04 | **典型用例记录**（zhuzhao-integration.md §4 新增）：审批通过 → activelist 加值 + taskrunner 下发任务 + 业务平台操作——链路映射（审批落 L1 / activelist 加值归 zhuzhao 网关 / 组合动作回调）+ **事务性约束**：加值与下发任务跨 activelist/taskrunner 两个独立系统无法强事务，采用 **L2 Outbox**（ADR-001 预留）——审批事务原子写「业务 + L1 + Outbox 两条命令」→ Relay 至少一次投递 → `task_id` 终身幂等 + activelist 幂等键收敛；唯一新设计点 = activelist 加值幂等键；taskrunner / activelist 零改动 |
| 2026-09-03 | 拍板定案（所有者）：**回调鉴权不做独立机制**（§4 安全边界 + §10 ⚠️ 关闭该项）——信任边界 = 内网网络隔离（与 API 暴露边界同级）+ `callback_url` 由 zhuzhao 提交时自行指定；zhuzhao 侧可选 URL 密钥路径段增强（capability URL，taskrunner 零感知）。zhuzhao 侧 16 号 P5 撤销、P6 随 M2 定案同步（不做前置校验） |
| 2026-09-03 | 拍板定案（所有者，P7）：**回调响应不引入状态字段**——2xx 仅代表执行完全成功；zhuzhao handler 业务失败直接映射 HTTP 状态码（不可重试 4xx / 可重试 5xx，常规 errcode）；taskrunner 现有 2xx/4xx/5xx 判定即最终行为（callback client 注释随下次提交更新）。zhuzhao 侧 16 号 P1–P7 同日全部关闭，M-E 动工前决策面清零 |
| 2026-09-03 | 登记全链路关联小改（随 M3）：callback client 回调请求带 `X-Request-ID: <payload.request_id>` 头（有则带，cron 触发为空则不带）——zhuzhao 入站 RequestID 中间件接受入站同 rid，回调链路与 `job_runs` 贯通（zhuzhao 03-audit-l2 §3.4 全链路矩阵） |
| 2026-09-03 | **公共能力统一基线**（所有者拍板，SSOT = zhuzhao 16 号 §9）：同级内部服务策略必须一致——鉴权统一**零认证 + 专用 network 拓扑**（覆盖 M2 静态 Bearer 定案，时序=先拓扑后拆，C2/C3）；访问日志统一中间件（X-Request-ID 读头/回显 + X-Operator 兜底，C1 关 B2）；`/readyz`（C4）、TZ（C5）、配置形态（C6 可选）；对齐清单 C1–C6 落 §10 |
| 2026-09-03 | 登记重叠执行策略缺口（§10）：cron 触发无 overlap 控制，仅幂等兜底——建议 job 增 `overlap_policy: allow \| skip_if_running`（默认 allow），随 M3/M4 落地，对齐 zhuzhao 13 号「阻塞策略按任务拍板」 |
| 2026-09-03 | **B3 拍板：存储统一 PG**——job_runs 迁独立 PG 数据库（C7，utils `postgres` 复用、schema 不变、约半天，随 M3/M4；§6/§8 同步）；SQLite 保留为 M1/M2 已交付实现；解除单副本约束（多副本按运维需要）；C4 readyz 改检 Redis+PG |
| 2026-09-03 | **AK/SK 基线修订**（所有者拍板，SSOT = zhuzhao 16 号 §9）：服务间通信统一 **AK/SK HMAC 签名**（utils `aksk` 包 C8 先行；C2 = Bearer→验签、C9 = 回调签名；覆盖当日「零认证+拓扑」与「回调不做鉴权」两条早前拍板；capability URL 作废；专用 network 降为第二道防线） |
| 2026-09-04 | **微服务结构重构**（所有者拍板：以正式微服务标准建设，内部与 zhuzhao 同规格——zhuzhao 16 号 §9「工程结构」基线）：目录重排 `cmd`（薄入口）/ `internal/app`（**Wire DI** + 生命周期）/ `handler`（薄 HTTP 层）/ `service`（TaskService 业务下沉 + submit/cron）/ `repository`（原 store）/ `middleware`（C1 访问日志 + C2 AK/SK 验签）/ `worker` / `callback`；config 改 **yaml + env**（C6，viper，全 env 兼容；密钥环空拒绝启动 fail-closed）；**C1/C2/C4/C5/C9 同批收口**（统一访问日志中间件含 rid 回显与 operator 兜底；API 验签 Bearer→AK/SK；/readyz 检 Redis+SQLite；Dockerfile TZ；回调以自身 SK 签名+rid 透传）；Makefile 门禁（lint=vet+gofmt / test / build）；handler 测试 ×7 重写为 aksk 口径 + C9 签名测试；C3（网络拓扑）/C7（迁 PG）仍待部署批次 |
| 2026-09-04 | **全仓审计修复**：① job_runs.request_id 跨查链断裂（API 提交/触发 body-only 绑定 + zhuzhao client body 未带 → 恒空）——双侧修复（handler 头值兜底 + client body 补 request_id）；② 容器崩溃循环（默认 configs/config.yaml 缺失即 fatal）→ env-only 模式 + Dockerfile 拷贝 configs；③ fail-closed 扩至 self_sk（回调签名身份缺失=zhuzhao 验签必 401）与空 queue；④ wire 清理对补齐（client/inspector/readyz-redis Close）；⑤ 本表 action_id 参数名/readyz 行/job_id 过滤/Scheduler 措辞修正 + README quickstart |
| 2026-09-04 | **能力目录（capability_registry）方案待定**（§4 新增子节）：现状 = 任务定义带提交方指定 `callback_url`、无跨服务能力发现；方案（**未拍板**）= 执行端自注册 code→路由入 taskrunner DB，提交只认 `action_code`、提交时解析快照、两级路由（跨服务目录 + 服务内 Registry），显式 `callback_url` 保留覆盖；开放点：注册表归属 / 注册方式 / 解析时机 / 覆盖保留 / 与密钥环多对端合并——待讨论定稿 |
| 2026-09-04 | **回调消息体约定 + 统一 body schema**（§4 回调契约补充）：业务参数 `params` 统一走回调 body（唯一业务负载通道、进 AK/SK 签名）；路由标识走路径、链路标识走 header、鉴权走签名层——各归其位；统一回调 schema `{task_id, request_id, params}` 为执行端 SDK 入口约定，加能力 = 注册 code + 写 Handler 回调入口零改动；params 存储（Asynq payload + job_runs）与传输（body）分离 |
| 2026-09-07 | 目标架构注记入档（§2/§4）：zhuzhao 演进为 **API 网关 + IAM**（薄网关，不持业务能力），业务数据/能力下沉各服务；动作归属泛化为「能力属主服务」——各服务挂自己的动作端点，taskrunner 统一调度（xxl-job 一调度中心 + N 执行器形态），handler 随数据迁移、taskrunner 仅改路由指向；多服务时代启用 owner_service / 多调用方 credential 预留；与「能力目录」方案（端点自注册）互为表里 |
