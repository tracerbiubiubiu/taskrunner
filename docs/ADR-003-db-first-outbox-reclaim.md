# ADR-003: 提交改 DB-first——job_runs 兼任 Outbox（queued 标志 + Reclaim 扫描器）

## 日期
2026-09-17（同日完成五轮评审修正合入）

## 状态
提议（计划定稿，待实施；落地后改「已采纳」并在 taskrunner.md §12 记变更）

## 背景
现状 `internal/service/submit.go` 的提交顺序是 **GetByTaskID → Enqueue → InsertPending**（入队优先）。当 Enqueue 成功但 InsertPending 失败时，任务真实执行但在 job_runs 中永久隐形：无法取消/重试、审计断档、同 task_id 重提会二次执行（A1 去重失效），且 warning 不回传调用方。

业界口径：跨异构存储（Redis + DB）不存在传统分布式事务——XA/2PC 要求双方都是实现 prepare/commit/rollback 协议的事务资源管理器，Redis 没有。可行范式是 **at-least-once 投递 + 幂等消费 + 单一事实源（SSOT）派生重建 + 对账兜底**。本 ADR 取 Transactional Outbox 的简化形态：job_runs 自兼任 outbox。

曾有一版草案（DB-first + `status=pending` 兼任 outbox 信号 + stale pending 扫描器，原稿 `.trae/documents/simplified-transactional-outbox.md`，经评审并入本 ADR 后已移除）。方向正确，但有一个承重墙级缺口：**`status=pending` 区分不了三种「待处理」**——从未入队 / 已入队排队中 / 已执行但 MarkRunning 失败。第三种（执行窗口恰逢 DB 抖动）会使行永久卡 pending 且 TaskID 已释放，扫描器每轮重投 → **系统性重复回调**；且并发重提撞 `task_id UNIQUE` 会从幂等 200 回归为 500。

本 ADR 为合成终稿：在原草案骨架（cronRunner 接线、三步 submit、扫描器范式）上合入八轮评审共 57 项发现（自查 5 项 + 外部评审 8 项 + 终审 5 项 + 复审 13 项 + 三审 9 项 + 四审 7 项 + 五审 5 项 + 六审 5 项，含各轮对前轮的勘误与结论修订；另有两条兼容性复验通过：其一（F35 的改动）为对现有 CancelTask 的纯增量变更、其二 row-value 游标 SQLite≥3.15/PG 均支持且 pgq 转换不受影响），修正记录见文末「评审修正记录」。

## 决策
1. **记录先行（DB-first）**：Submit 改为 **GetByTaskID → InsertPending → Enqueue**。job_runs 行先于 asynq 任务存在，行即事实源（SSOT），Redis 任务是可重建的投影。

2. **显式 `queued` 标志（正确性承重墙；投递状态标志，两义：已交付 Redis / 已了结——第三域清理确认，评审 F54）**：job_runs 加 `queued` 列（历史行默认 1 =「已尝试过，**第一轮永不触碰**；第二轮仍会盘点它们——存量 pending 行升级时被告警属预期口径，上线前盘点见实施计划 7，评审 F41」；新行写 0；入队成功翻 1）。命名注记：与既有 `enqueued_at`（**受理时间**，InsertPending 时写）词根划清。**两义定义（评审 F54）**：`queued=1` = 已交付 Redis（含 Retention 留观中）**或** 已了结（第三域确认 Redis 键消亡，评审 F45/F49——仅 DeleteTask 成功或返回 `ErrTaskNotFound` 后置 1，active/瞬时错误不出域）；`queued=0` = 尚未交付。两义证据等级不同：前者是投递事实，后者是消亡确认——**无确认的置位不在此列**（如取消时直接翻标志，F37 否决）。**自动重投只认 `queued=0 AND status='pending'`（从未入队且未离开待执行的安全域）；已取消行（F1）与终态残留行（F14）都被排除在自动重投之外——终态残留行无害，不重投也无需告警（评审 F42），已取消行按用户意图静默（取消失效空洞由第三域闭合，决策 4）。** 取消路径**不翻** queued（评审 F37：status 过滤已承担扫描域互斥，翻标志冗余且会使从未入队的取消行呈 queued=1，污染单一语义）。
   另加 `enqueue_payload` 快照列（task.Payload JSON）作重投唯一来源——自带 timeout_secs，规避「job_runs 无超时覆盖」的重建缺口，零触碰**既有** SELECT/Scan（新列的读取只发生在新方法与 UPDATE 影响行数里；`Run.Queued` 字段仅由新方法填充，既有查询下恒为零值、不可作判据，评审 F29），Payload 演进免迁移。**快照与 asynq 载荷同源**：submit 侧一次 `json.Marshal(p)`，同一份 bytes 既写 `enqueue_payload` 又建 asynq 任务（评审 F9）。

3. **Submit 三步语义**（保留内联快路径，任务执行零延迟变化）：
   - A1 查重命中 → `(false, nil, nil)`（幂等重提，不变）；
   - A2 InsertPending(queued=0 + 快照)。**UNIQUE 并发重复采用驱动无关判定**（评审 F23）：InsertPending 失败后**复查 GetByTaskID**，命中 ⇒ 并发重提 → `(false, nil, nil)`（幂等 200）；未命中（含复查失败）⇒ 真错误 → `(false, nil, err)` → 500 干净失败（此刻 Redis 无任务、DB 无行）；
   - A3 Enqueue（opts 含 **asynq.Retention**）：成功 / `ErrTaskIDConflict` → **按状态条件标记** `UPDATE job_runs SET queued=1 WHERE task_id=? AND status='pending'`：
     - 1 行 → `(true, nil, nil)`；
     - **0 行 = 行在入队期间离开 pending，三义（评审 F39，各自后果不同）**：并发取消 → 立即 `Inspector.DeleteTask` 撤销刚入队的任务；行已被 worker 取走（running）→ DeleteTask 失败于 active，无害放弃（MarkRunning 将拒绝、回调照发，不可再缩的 at-least-once 残余）；行已达终态（极端调度下整次回调先于标记完成）→ 撤销无意义，DeleteTask 仅清 Retention 留观副本，无害。**补偿 DeleteTask 一并容忍 `ErrTaskNotFound`**（并发补偿/取消已删的先来者，评审 F44——先例即 CancelTask 的容忍模式）。撤销成败不影响对调用方的受理口径；
     - 标记**执行失败**（DB 写故障，区别于 0 行）→ 不标记，行留 queued=0，**由第一轮的冲突分支收敛**（任务已在 Redis，下轮重投撞 `ErrTaskIDConflict` → 条件标记补课；评审 F33——「已投未标记」集合是扫描器组件，A3 不写集合，改稿原文「交集合自愈」作废）；
     - 其他失败（Redis 抖动）→ Warn、不标记、返回 `(true, warning, nil)`——行已落库即 SSOT，扫描器修复后执行；200 语义 =「已受理，稍后执行」。
   - **warning 明确不回传响应体**（评审 F8）：`SubmitOutput` 保持 `TaskID/Accepted` 不变，warning 仅进日志；调用方需要知道是否真的入队时查 `GET /v1/tasks/{id}` 的 live_state——避免改响应契约。

4. **ReclaimLoop 扫描器**（仿 cron.go 范式：Run 薄壳 + 同步单轮方法；经 `cronRunner` 接口接线）：
   - **第一轮（自动修复，仅安全域）**：判据 `queued=0 AND status='pending' AND enqueued_at<=now-stale_after`——`status='pending'` 不可省（评审 F1/F14：缺它有两类复活——已取消行被重投执行；以及「内联入队成功、标记失败、DB 瞬时抖动在执行前恢复」残留的 `succeeded+queued=0` 行，在 Retention 过期后被二次入队造成重复回调）。取批 → 反序列化快照 → **判空防御**（评审 F7：空快照跳过 + Warn，纵深加固，正常不可达）→ Enqueue（**opts 与 A3 完全同参**，评审 F6：`asynq.Queue`/`MaxRetry`/`Timeout`（快照 TimeoutSecs 还原）/`Retention`）→ **Enqueue 结果三分（评审 F32——冲突分支是「A3 入队成功后标记失败/进程在标记前崩溃」的主恢复路径，不可缺省）**：
     - **成功 / `ErrTaskIDConflict`** → 按状态条件标记（与 A3 同构）：1 行 → 完成（冲突分支正是「A3 入队成功后标记失败/进程在标记前崩溃」的收敛路径）；0 行（行在标记前离开 pending，**三义同 F39**——worker 可在刚入队后立即取走，非仅并发取消）→ DeleteTask 补偿（**容忍 `ErrTaskNotFound`**，active 放弃；评审 F44）；标记执行失败 → task_id 入**「已投未标记」内存集合**——**集合写者唯一为扫描器**（评审 F33）：集合成员下轮**只重试标记、跳过重投**（任务已在 Redis，重投纯属制造重复），标记成功出集；**成员标记返回 0 行（已被取消，三义同 F39）→ DeleteTask 补偿（容忍 `ErrTaskNotFound`/active 放弃，评审 F44）+ 出集**（评审 F34：否则成员永久驻留、集合无界增长）。**`ErrTaskIDConflict` 不计入连续失败计数**（它意味着任务已在 Redis、行即将收敛，误报且阻断收敛）；
     - 非冲突失败（Redis 不可用等）→ Warn + 连续失败计数，同一行 ≥3 次 → Error（接入「Error 即告警」约定，出处 taskrunner.md §6:229）。
   - **第二轮（仅告警，绝不自动重投；按 State 分流，评审 F20）**：判据 `status='pending' AND queued=1 AND enqueued_at<=now-stale_after`（阈值复用 stale_after，评审 F17①），**复用 reclaim.batch + 游标轮转推进**（评审 F36：否则在途积压行每轮按 enqueued_at 升序占满批位，排在其后的 completed/absent/archived 异常行永不入选——告警饥饿；游标 `(enqueued_at, task_id)` 每轮推进、耗尽回卷，保证 eventual 全覆盖，每轮 GetTaskInfo 调用上界 = batch）。查证分流：
     - `absent` → Error「行 pending 但任务不在队列（Redis 丢失/标记残留），需人工处置：重新提交（新 task_id）或等 M4 对账」（评审 F21：原「需人工 retry」不可操作——RetryTask 只接受 failed/dead，pending 行 409）；
     - `completed` → Error「任务已执行完但 job_runs 未落终态——标记/终态写入故障的直接指纹，需人工补录」（评审 F20：completed 不是在途积压，**立即**告警而非等 Retention 过期）；
     - `archived` → Error「asynq 已放弃重试且行仍 pending，永不自行推进」；
     - 其余（pending/scheduled/retry/active/aggregating）→ 在途，静默跳过。
     第二轮保留原草案「Redis 丢任务」的可见性优势，但**只暴露、绝不重投**，不制造双投递。
   - **第三域（仅清理，绝不重投；评审 F45，出域条件经 F49 统一为条件式）**：`status='canceled' AND queued=0 AND enqueued_at<=now-stale_after` 的行，取批（**复用 reclaim.batch + 同款 `(enqueued_at, task_id)` 游标轮转**，评审 F50/F56——队头取消行 DeleteTask 持续失败（active/Redis 不可用）时同样会占批位，F36 的游标判据同样适用）→ 逐行 DeleteTask 探针 → **按结果分流（评审 F49）**：`DeleteTask 成功` 或 `ErrTaskNotFound`（键已消亡）→ `MarkCleanupDone` 置 `queued=1` 出域；`active` 或其它错误（含瞬时 Redis 错误）→ **不出域**，下轮重探——无条件出域会在瞬时错误下放走仍在 Redis 的已取消任务，重开 F45 要闭合的空洞。**闭合「取消后、补偿前进程崩溃（或补偿 DeleteTask 遭遇非 active/非 NotFound 的瞬时错误）→ 已取消任务仍在 Redis 并照常执行」的取消失效空洞**——该形态下行是 canceled+queued=0，原本两个扫描域都不覆盖、无自愈；不出域标志则每 tick 对全部历史取消行重探、成本无界。
   - 单 goroutine 串行，无需 SKIP LOCKED；停机 = ctx 取消。
5. **Retention 入队选项**（默认 1h）：完成任务在 Redis 留观占住 TaskID，挡扫描器迟到重投与任务完成释放的竞态。配置名 `reclaim.retention`：虽作用于 Submit 入队 opts，因扫描器依赖其判断 TaskID 是否释放而归入 reclaim 组（评审 F11/F17②）。
6. **取消行不进入两个重投/告警域，另有仅清理域（评审 F2 → F19 → F37 → F52，标题经 F52 修正以涵盖第三域）**：两个重投/告警域排除已取消行由判据的 `status='pending'` 承担（F1）；在途投递由补偿握手回收（F19：条件标记 0 行 → DeleteTask）；专用清理域（决策 4 第三域）只对已取消行做 DeleteTask 探针、绝不重投。**`MarkCanceledIfPending` 保持现 SQL 不动、不翻 queued**（评审 F37，理由经 F54 重述：翻标志**冗余**——status 过滤已承担扫描域互斥，且属**无确认断言**——取消动作本身不证明 Redis 键消亡，与第三域「确认后置位」证据等级不同；F2 原方案作废）。残余竞态收敛到「DeleteTask 遇 active 态」不可再缩点。连带：取消路径对 `DeleteTask` 的 `ErrTaskNotFound` 容忍注释更新为「queued=0 时任务不在 Redis 属常态」；**CancelTask 对 GetTaskInfo 命中 `completed`/`archived` 的行改判 409**（评审 F35，**取代 F24 的「可接受」结论**：删留观并标 canceled 会抹掉 F20 的告警指纹，并把「任务已执行、回调已发」写成 canceled 假终态——查询方将误判为从未执行；409 文案说明记录异常需人工介入）。

## 理由
- 结构性消除「执行但无记录」：插行失败 = 500（Redis 无任务、DB 无行，干净）；入队失败 = 行在（可见、可修、可查）。原隐性权衡（执行优先 vs 记录优先）不再需要二选一。
- `queued` 标志是方案正确性的前提：没有它，三态不可分，扫描器必然对「MarkRunning 失败的已执行行」系统性重投。F1/F2/F14/F19 进一步表明：**取消与终态残留都必须与扫描域互斥**，且「pending 且 Redis 查不到 → 准予取消」这一既有准入判据在 DB-first 下语义反转（「查不到」从「永不执行」变成「尚未入队」）——补偿握手（按 status 条件标记 + 0 行撤销）是把判据重新对齐语义的最小改动。
- MarkQueued 按 `status='pending'` 条件标记（终审 F19 修订 F4 的「无条件」结论；三审 F32/F33 进一步把对称性补齐到第一轮）：无条件写会**掩蔽并发取消信号**——0 行是「行已离开 pending」的唯一即时指纹。此前外评反对的是按 `queued=0` 条件（其双投递论证不成立，维持驳回）；按 **status** 条件职责不同：它不是幂等手段，是取消检测器。
- 扫描器第一轮必须显式三分 Enqueue 结果（三审 F32）：`ErrTaskIDConflict` 在本设计中不是异常而是**崩溃恢复的正常信号**（「入队成功、标记前崩溃」的主恢复路径），把它混入失败分支会导致行永不收敛、Retention 过期后二次入队、且 F3 防线（集合）因永不填充而被整体旁路。
- 快照列优于归一化列（timeout_secs 方案）：后者需全量改动所有 rows.Scan（16→17 列）且 Payload 每加字段需再迁移；前者两列、零触碰既有查询、前向兼容。
- **被替代的机制**：评审讨论中曾提出的补偿删除、worker 缺行自愈两个替代方案（仅存于讨论与已删草案，代码中无对应物，评审 F18）被 DB-first 结构性消除，不再实施；M4 对账任务维持后置，作终兜底。

## 后果
- 正面：缺行窗口关闭；并发重提幂等不回归；Redis 抖动时提交 200 + 行可见可修；Redis 丢任务有告警出口；取消与投递的竞态经补偿握手收敛到不可再缩点；扫描器顺带自愈「入队失败残留」与「标记前崩溃残留」。
- 负面/风险：
  ① **双投递残余**（不可再缩点 + 两个 >Retention 变体）：并发取消恰逢 worker 取走（补偿 DeleteTask 报 active 态，放弃补偿）；扫描器停摆超 Retention；或 DB 读可用写不可用超 Retention（SQLite 磁盘满形态）——后者已由「已投未标记」集合把「每 tick 回调风暴」降为「重投暂停 + Error 告警」；集合重启丢失期间由第一轮冲突分支接手补标记，**写故障持续期 < Retention 内风暴不复现；超 Retention（Redis 键已释放）则退化为「停摆超 Retention」同类变体**（重投成功 → 重复回调），评审 F47。M4 对账兜底 + 告警介入。
  ② 200 语义在 Redis 抖动场景从「已入队」弱化为「已受理待入队」（对接口径写入 taskrunner.md §5；warning 不回传已在决策 3 明确）。
  ③ 新增常驻组件（扫描器）与配置面，需关注积压告警；连续失败计数与「已投未标记」集合均为**进程态**——重启清零、多副本各自计数（多副本本身安全：同 task_id 竞投被 Redis TaskID 冲突挡住，无需 cronloop 式分布式锁，评审 F30）。
  ④ completed 态指纹在 Retention 留观期内即可被第二轮识别（F20 分流后不再有 1h 延迟）；留观过期后 completed 退化为 absent，告警文案不同、时效相同。
  ⑤ **登记不变式（评审 F38）**：「`ErrTaskIDConflict` = 已投递」的判定隐含 **job_runs 行生命周期 ≥ 同 task_id 的 Redis 键生命周期（含 Retention 留观）**——当前行终身保留成立；M4 保留期清理落地时必须重估（清理不得早于 Redis 键消亡 + Retention）。
  ⑥ **既存空洞登记（三审残余项）**：`running + queued=0`（MarkRunning 成功但 Finish 长期失败）不落在任何扫描域——原稿即有的边界；依赖 Finish 失败自身 Error 日志与 M4 对账覆盖，本设计不为此扩域。
- 后续跟进：M4 对账任务（终兜底）+ 手动 reclaim 端点备选（F21）；接入方上量/多接入方/审计要求「记录与队列强一致」时再评估完整 Outbox（独立 relay；Debezium/CDC 对当前规模过重）。

## 评审修正记录（57 项，全部合入）

| ID | 发现 | 处置落点 |
|---|---|---|
| F1 | 第一轮判据缺 status='pending'，已取消行被复活重投 | 决策 4 第一轮判据 |
| F2 | 取消后竞态照常投递 | 决策 6（F19 补偿握手；其「翻标志」子项经 F37 删除——冗余） |
| F3 | 标记持续失败 → DB 写故障期每 tick 回调风暴 | 决策 4「已投未标记」集合 + 后果① |
| F4 | ~~MarkQueued 定为无条件更新~~ **被 F19 修订**：按 status 条件标记 + 0 行补偿 | 决策 3 A3 / 理由 |
| F5 | 第二轮查询无索引可命中（开口区间等效全表扫） | 实施计划 1 第二个 partial index |
| F6 | ReclaimLoop 缺 Queue/MaxRetry/Retention，重投与内联不同参 | 决策 4 第一轮「同参」 |
| F7 | 快照判空防御（正常不可达，纵深加固） | 决策 4 第一轮 |
| F8 | warning 不回传响应体未明确 | 决策 3 末条 |
| F9 | 快照与 asynq 载荷同源（一次 Marshal bytes 双用） | 决策 2 |
| F10 | 配置细则：≤0 回退默认（cron.go 零值防御先例），可选 tick<1s 拒启 | 实施计划 4 |
| F11 | retention 归组语义注解 | 决策 5 |
| F12 | `enqueued` 与 `enqueued_at`（受理时间）同词根混淆 → 标志改名 `queued` | 全文（决策 2 命名注记） |
| F13 | Retention 遮蔽第二轮告警（F20 分流后弱化为文案差异） | 决策 4 第二轮 / 后果④ |
| F14 | `succeeded+queued=0` 残留（瞬时抖动形态）在 Retention 过期后被二次入队——status 过滤挡的不止取消 | 决策 4 第一轮 / 理由 |
| F15 | 第三处同语义文案 cron.go:92（cronloop）漏列 | 实施计划 6 |
| F16 | 章节引用偏差（实际 §5/§6，非 §4） | 实施计划 7 / 关联文档 |
| F17 | ①第二轮阈值复用 stale_after；②retention 命名与 F11 合并；③「Error 即告警无出处」不成立（出处 §6:229） | 决策 4/5 / 实施计划 7 |
| F18 | 「补偿删除/worker 自愈」仅存于讨论与草案——理由措辞自包含 | 理由 |
| F19 | **P0**：CancelTask 准入判据语义反转，内联 A3 在途 Enqueue 竞态未被覆盖 → 取消后任务仍执行 | 决策 3 A3 / 决策 6 / 理由（修订 F4） |
| F20 | **P1**：completed 是「已执行完未落终态」指纹，应立即告警而非静默跳过 | 决策 4 第二轮按 State 分流 |
| F21 | **P1**：「需人工 retry」不可操作（RetryTask 只接受 failed/dead） | 决策 4 第二轮文案 |
| F22 | **P1**：Service 加 Retention 字段；装配两处（provideSubmitter + main.go:130 CLI） | 实施计划 2/5 |
| F23 | **P1**：UNIQUE 判定缺实现前提 → 驱动无关「失败后复查 GetByTaskID」方案 | 决策 3 A2 |
| F24 | **P1**：Retention×CancelTask 审计——~~completed 态取消可接受~~ **被 F35 取代**：改判 409 | 决策 6 |
| F25 | **P2 勘误**：cron.go:92 实为 Warn 非 Error（外评笔误），维持 Warn 级 | 实施计划 6 |
| F26 | **P2**：`-run TestPg` 大小写不匹配会静默跑 0 个 PG 用例 → `-run TestPG` | 实施计划·验证 |
| F27 | **P2**：§10 无「风险窗口表」 | 实施计划 7 |
| F28 | **P2**：§12 需显式标注取代 2026-09-03「先入队后落库」决策 | 实施计划 7 |
| F29 | **P2**：新列同进 CREATE TABLE；「零触碰」限定既有查询；Run.Queued 零值语义 | 决策 2 / 实施计划 1 |
| F30 | **P2**：不变式 stale_after < retention；计数器内存态；多副本安全声明 | 后果③ / 实施计划 4 |
| F31 | **P2**：fakeEnqueuer 全新编写（全仓无 Enqueuer 假件） | 实施计划·测试 |
| F32 | **P0（三审）**：第一轮 Enqueue 结果未三分——冲突是崩溃恢复主路径（「入队成功、标记前崩溃」），误入失败分支 → 行永不收敛、Retention 过期后二次入队、F3 集合被旁路 | 决策 4 第一轮三分 + 冲突不计失败计数 |
| F33 | **P0（三审）**：A3「交集合自愈」不成立——集合是扫描器组件，A3 不写；A3 未标记行的真实收敛路径是第一轮冲突分支 | 决策 3 / 决策 4（集合写者唯一化） |
| F34 | **P1（三审）**：集合成员 0 行处置缺失 → 永久驻留、集合无界增长 | 决策 4 第一轮（0 行 → DeleteTask 补偿 + 出集） |
| F35 | **P1（三审）**：取消抹掉 completed 指纹 + canceled 假终态 → CancelTask 对 completed/archived 命中改判 409（取代 F24） | 决策 6 |
| F36 | **P1（三审）**：第二轮升序 + LIMIT 队头阻塞 → 在途行占批位，异常行告警饥饿 → 复用 batch + 游标轮转 | 决策 4 第二轮 |
| F37 | **P2（三审）**：MarkCanceledIfPending 翻 queued=1 冗余（两轮判据含 status 过滤）且污染 queued 单语义 → 删除 | 决策 2/6 / 实施计划 1 |
| F38 | **P2（三审）**：登记不变式「job_runs 行生命周期 ≥ Redis 键生命周期（含 Retention）」，M4 清理落地时重估 | 后果⑤ |
| F39 | **P2（三审）**：A3 补偿「0 行」实为三义（并发取消 / 已取走执行 / 已完成），文案注明各自后果 | 决策 3 A3 |
| F40 | **残余登记（三审）**：`running + queued=0`（Finish 长期失败）不落任何扫描域——既存空洞，依赖 Finish Error 日志 + M4 对账 | 后果⑥ |
| F41 | **P2（四审）**：「历史行扫描器永不触碰」与第二轮判据矛盾——存量 pending+queued=1 行升级即被命中批量告警；措辞改「第一轮永不触碰」+ 实施计划补上线前存量盘点 | 决策 2 / 实施计划 7 |
| F42 | **P2（四审）**：终态残留行「只告警」无落点（不在任何扫描域）→ 改「不重投、无需告警（无害残留）」 | 决策 2 |
| F43 | **P2（四审）**：第一轮 0 行仍按单义写——worker 可在刚入队后立即取走，三义同 F39 → 对齐措辞 | 决策 4 第一轮 |
| F44 | **P2（四审）**：补偿 DeleteTask 需容忍 `ErrTaskNotFound`（并发补偿/取消已删；先例 CancelTask）——A3/第一轮/集合成员三处 | 决策 3 / 决策 4 |
| F45 | **P2（四审）**：取消失效空洞（取消后补偿前崩溃 → canceled+queued=0 行无域覆盖、已取消任务照常执行）→ 新增第三域清理探针（DeleteTask 容忍 NotFound、探针后置 queued=1 出域——不置标志则历史取消行每 tick 重探成本无界） | 决策 2/4 / 实施计划 1/3 |
| F46 | **P2（四审）**：游标 `(enqueued_at, task_id)` 与单列索引不匹配 → `idx_job_runs_stuck` 改复合；游标进程态重启回卷注释点明 | 实施计划 1/3 |
| F47 | **P2（四审）**：后果①「风暴不再复现」措辞过强——「重启丢集合 + 写故障 > Retention」下键已释放、重投成功 → 限定为「写故障期 < Retention 内不复现」 | 后果① |
| F48 | **P1（五审）**：第三域「出域」写入无对应 store 方法——MarkQueued 的 `status='pending'` 条件是 F19 取消检测器，对 canceled 行恒 0 行不写 → 新增 `MarkCleanupDone`（`WHERE task_id=? AND status='canceled' AND queued=0`） | 实施计划 1 / 决策 4 |
| F49 | **P1（五审）**：出域条件两处表述矛盾（决策 2 条件式正确、决策 4 读作无条件）→ 统一为「仅 DeleteTask 成功或 ErrTaskNotFound 才置 1；active/瞬时错误留域下轮重探」+ 测试断言 | 决策 2/4 / 测试 |
| F50 | **P2（五审）**：第三域缺批次上界，单 goroutine 下会挤占第一/二轮 tick 预算 → ListCanceledUnqueued 加 LIMIT 复用 reclaim.batch | 决策 4 / 实施计划 1 |
| F51 | **P2（五审）**：评审修正记录标题「40 项」未随表格更新（表已 F1–F47） | 本节标题 |
| F52 | **P2（五审）**：决策 6 标题「取消与扫描域互斥」与第三域（专门扫描 canceled 行）字面矛盾 → 改「取消行不进入两个重投/告警域，另有仅清理域」 | 决策 6 |
| F53 | **P2（六审）**：评审记录标题「47 项」未随 F48–F52 追加更新（表实为 52 行，grep 计数核实）——**F51 同类错误原地复发** → 改 52 | 本节标题 |
| F54 | **六审**：决策 2「单一语义」与第三域置位自相矛盾——F37 以「污染单一语义」否决的形态（从未入队的取消行呈 queued=1）被 F45 原样引入（探针 NotFound 后置 1）→ 采方案 (a)：改「两义：已交付 Redis / 已了结」，F37 理由重述为「冗余 + 无确认断言」（两者证据等级不同：投递事实 vs 消亡确认）；方案 (b) 独立 cleanup_done 列因文档纯性引入 schema 成本，过度设计，弃 | 决策 2 / 决策 6 / 表 F37 |
| F55 | **P2（六审）**：行号引用脆且外评 ±1 修正不可复现（grep 实证当前 :229=日志级别约定、:187=受理语义，:230 才是脱敏与截断边界）→ 保留数字、加条目名锚点防并发编辑漂移 | 实施计划 7 / 关联文档 |
| F56 | **P2（六审）**：第三域缺游标——F36 判据未贯彻（队头取消行 DeleteTask 持续失败占批位，阻塞后续取消行清理）→ ListCanceledUnqueued 加同款游标；idx_job_runs_cancel_cleanup 升级复合 `(enqueued_at, task_id)` | 决策 4 / 实施计划 1 |
| F57 | **P2（六审）**：背景兼容性复验句的 F35 表述易混（F35 是发现项又被当复验引用）→ 改「其一（F35 的改动）为…」 | 背景 |

## 实施计划

### 改动清单
1. `internal/repository/store.go`：
   - **新列同时进三处**（评审 F29）：schema 常量（SQLite CREATE TABLE）、schemaFor（PG DDL）、migrateColumns（存量库幂等补列）——`queued INTEGER NOT NULL DEFAULT 1`、`enqueue_payload TEXT NOT NULL DEFAULT ''`；
   - 两个 partial index（两驱动均支持）：`idx_job_runs_unqueued ON job_runs(enqueued_at) WHERE queued = 0 AND status = 'pending'`（第一轮，谓词与判据对齐）、`idx_job_runs_stuck ON job_runs(enqueued_at, task_id) WHERE status = 'pending' AND queued = 1`（第二轮，评审 F5；**复合列匹配游标谓词与排序**，评审 F46）；第三个 partial index：`idx_job_runs_cancel_cleanup ON job_runs(enqueued_at, task_id) WHERE status = 'canceled' AND queued = 0`（第三域，评审 F45；**复合列匹配游标**，评审 F56——探针后置 1 出域，索引只含未清理的近期取消行，体积有界）；与既有 `idx_job_runs_enqueued_at` 谓词正交、可并存；
   - Run struct 增 `Queued bool` / `EnqueuePayload string`（**注**：Queued 仅由新方法填充，既有查询下恒为零值、不可作判据，评审 F29）；InsertPending 写入（payload 同源，见 2）；InsertPending 维持统一 `%w` 包装（F23 走复查方案，不引入 sentinel/驱动错误类型耦合）；
   - 新方法：`MarkQueued`（**按状态条件** `UPDATE job_runs SET queued=1 WHERE task_id=? AND status='pending'`，返回影响行数供调用方补偿判定，评审 F19）；`ListUnqueued`（`queued=0 AND status='pending' AND enqueued_at<=?`）；`ListStuckPending`（`status='pending' AND queued=1 AND enqueued_at<=?`，**带游标参数** `(enqueued_at, task_id)` 支持轮转，评审 F36）；`ListCanceledUnqueued`（第三域：`status='canceled' AND queued=0 AND enqueued_at<=?`，**带同款游标参数 + LIMIT**，批次复用 reclaim.batch，评审 F45/F50/F56）；`MarkCleanupDone`（**canceled 域专用置位**：`UPDATE job_runs SET queued=1 WHERE task_id=? AND status='canceled' AND queued=0`——**不可复用 MarkQueued**，其 `status='pending'` 条件是 F19 的取消检测器，对 canceled 行恒 0 行不写，评审 F48）；
   - **`MarkCanceledIfPending` 保持现 SQL 不动、不翻 queued**（评审 F37：两轮判据含 status 过滤后翻标志冗余且污染单一语义），仅更新其注释口径（决策 6 连带）。
   - pgq 注意：字符串字面量禁 `?`（`status = 'pending'` 这类字面量安全）。
2. `internal/service/submit.go`：按决策 3 重排为三步；`json.Marshal(p)` 提前到 InsertPending 之前，同一份 bytes 填 `Run.EnqueuePayload` 与 asynq task（评审 F9）；**service.Service 结构体新增 `Retention time.Duration` 字段**（A3 opts 用，评审 F22）；头部注释更新为「DB-first（job_runs 兼任 outbox，见 ADR-003）」。
3. 新建 `internal/service/reclaim.go`：
   ```go
   type ReclaimLoop struct {
     Store      *repository.Store
     Client     Enqueuer        // 复用 asynq Client
     Inspector  TaskInspector   // 第二轮查证用，绝不投递
     Logger     *slog.Logger
     Tick, StaleAfter, Retention time.Duration
     BatchSize  int
     Queue      string
     MaxRetry   int
     Now        func() time.Time
     // 第一轮「已投未标记」集合与第二轮游标为进程内状态（非字段示意省略）
   }
   ```
   `Run(ctx)` ticker 薄壳 + `ReclaimStalePending(ctx)` 同步单轮：第一轮（判据 F1/F14 / 判空 F7 / 同参 F6 / **Enqueue 结果三分 F32**：成功与冲突 → 条件标记（1 行完成、0 行 DeleteTask 补偿、标记失败入集合），非冲突失败 → Warn + 计数，**冲突不计计数** / 已投未标记集合 F3，**成员 0 行 → 补偿 + 出集 F34**）；第二轮（按 State 分流 F20 + 游标轮转 F36，绝不重投）；**第三域清理探针 F45**（取批复用 batch F50；结果分流 F49——DeleteTask 成功/NotFound → `MarkCleanupDone` 出域，active/瞬时错误不出域下轮重探）。单 goroutine，停机 = ctx 取消；**游标与集合为进程态，重启回卷/清零——崩溃循环下第二轮队头可能长期占位，代码注释点明**（评审 F46）。
4. `internal/config/config.go` + `configs/config.yaml`：`reclaim.tick`（默认 10s）/ `reclaim.stale_after`（默认 30s）/ `reclaim.batch`（默认 100）/ `reclaim.retention`（默认 1h，归组理由见决策 5）；env `TASKRUNNER_RECLAIM_TICK/_STALE_AFTER/_BATCH/_RETENTION`；校验细则（评审 F10/F30）：四值 `<=0` 回退默认（cron.go 零值防御先例，不拒启），可选 `tick < 1s` 拒启（防 CPU 打爆）；**不变式 `stale_after < retention` 违反拒启**（否则第一轮重投窗口与 Retention 防线重叠，风暴防线失效）。
5. 装配（**两处同步**，评审 F22）：`internal/app/providers.go` `provideSubmitter` 传入 Retention + 新增 `provideReclaim(cfg, st, sub *service.Service, insp, logger)`（复用 sub.Client 与现有 Inspector）→ `wire_gen.go` 追加 → `app.go`：App 加 `reclaim cronRunner` 字段 + NewApp 参数 + `go a.reclaim.Run(ctx)`（cron 旁，同 ctx 生命周期）；**`cmd/taskrunner/main.go:130` CLI 手工字面量同步加 Retention**。
6. 漏项：语义翻转的 warning 文案共**三处**（评审 F15）：`internal/service/task_service.go` :138-142（Submit）与 :535-539（TriggerJob）、`internal/service/cron.go:92`（cronloop「submitted but job_runs insert failed」，**现为 Warn 级，维持 Warn**——终审 F25 勘误：外评称 Error 不实；自愈型瞬态不属告警口径）→ 统一改为「job_runs persisted but enqueue failed, reclaim loop will retry」；`cmd/taskrunner/main.go` CLI 同文案同步（:139-141）；`internal/worker/worker.go:85` 注释更新（「提交后未及落库」成因已消除，改为历史/手动残留口径，逻辑不动）；取消路径对 `DeleteTask` 的 `ErrTaskNotFound` 容忍注释更新为「queued=0 时任务不在 Redis 属常态」（决策 6 连带）。
7. `docs/taskrunner.md`：§10 后置项总表/运行时边界口径补双投递残余、Redis 丢失告警口径与 F38 不变式、F40 既存空洞登记；**上线前存量盘点（评审 F41）**：`SELECT count(*) FROM job_runs WHERE status='pending'` 预估第二轮告警量——存量 stuck pending 行升级即被告警属预期口径，或上线前人工处置；§5 受理语义（`POST /v1/tasks` 行，当前 :187）补「记录先行 + warning 不回传」；§6 终态/status 枚举处补扫描器 Error 口径（「Error 即告警」出处 = §6「日志级别约定」条目，当前 :229；**行号随文档演会漂移，以条目名为锚**，评审 F55）；**§12 变更记录实施落地时显式标注取代 2026-09-03「先入队后落库防孤儿行」决策**（评审 F28，否则与决策 1 矛盾无对冲记录）。

### 测试
- store_test：补列断言（含**新建库直接含新列**，F29）；InsertPending 写 0+快照；MarkQueued 条件标记——pending 行 1 行、**canceled 行 0 行**（F19 回归）；ListUnqueued 不含 canceled/终态行（F1/F14 回归）；ListStuckPending 判据、limit 与**游标轮转**（F36）；**MarkCanceledIfPending 不改 queued**（F37 回归：取消行 queued 保持 0，仍被 status 过滤排除）；PG 集成（`TASKRUNNER_TEST_PG_DSN` 门控）补迁移与新查询。
- service/submit_test：**新建 fakeEnqueuer（全仓无现成 Enqueuer 假件——fakeSubmitter 是 Submitter 假件，评审 F31）**，可编程返回成功/`ErrTaskIDConflict`/错误并记录调用；真 SQLite（仿 task_test.go 风格）。用例：A1 幂等；A2 UNIQUE→复查命中→(false,nil,nil) 且零入队（F23）；A2 真错误→err；A3 成功→queued=1；A3 冲突→queued=1；A3 失败→(true,warning,nil) 且 queued=0；**A3 标记 0 行（预置取消）→ DeleteTask 补偿被调用**（F19 核心用例）。
- service/reclaim_test（同步驱动单轮方法）：宽限外才扫；**冲突分支 → 条件标记收敛、不计失败计数**（F32 核心用例：模拟「入队成功标记失败后重启」——预置 queued=0 行 + fake 返回 Conflict，断言行翻 1 且离开扫描域）；成功标记；**标记执行失败→入集合→下轮只重试标记不重投→成功出集**（F3）；**集合成员被取消→0 行→DeleteTask 补偿+出集**（F34）；判空跳过 + Warn（F7）；batch 截断；非冲突失败连续 ≥3 → Error；第二轮按 State 分流——absent/completed/archived→Error（各文案）、在途态→跳过（F20）；**游标轮转：>batch 在途行 + 1 条异常行，轮转若干轮后异常行被检视**（F36）。
- 取消交互：取消后行不在两个扫描集（F1/F37 组合）；**CancelTask 对 completed/archived 命中 → 409 且不删留观**（F35）。
- **第三域（F45/F49/F50）**：canceled+queued=0 超时行 → DeleteTask 探针被调用；**DeleteTask 成功或 `ErrTaskNotFound` → `MarkCleanupDone` 置 queued=1 出域**；**active/瞬时错误 → 不出域、下轮重探**（F49 断言）；非超时行不探；批次上界生效（F50）；**MarkCleanupDone 对 pending 行返回 0 行**（F48 条件回归）。
- **补偿 DeleteTask 容忍 `ErrTaskNotFound`**（F44：A3/第一轮/集合成员三处同断言）。
- handler/server_test 回归（响应契约不变，fakeSubmitter 同步新语义）；config 校验新路径（F10/F30：≤0 回退、stale_after<retention 拒启）。

### 验证
```bash
go build ./... && go vet ./... && go test ./... -count=1
# 有 PG 时（注意用例名大写，评审 F26：-run TestPg 会静默匹配 0 个）：
TASKRUNNER_TEST_PG_DSN=... go test ./internal/repository/ -run TestPG -v
```
手动冒烟五步：① 正常提交 → accepted=true 且行已落；② 停 Redis 提交 → 200 + warning，行 pending+queued=0；③ 恢复 Redis → 等 reclaim 日志「re-enqueued」，任务执行；④ 同 task_id 重提 → accepted=false 幂等；⑤ 停 Redis → 提交 → **取消** → 恢复 Redis → 确认已取消任务不被执行（F1/F19 冒烟）。

分支 `feat/outbox-reclaim`（自 develop），规模约 800 行含测试。

## 关联文档
- `docs/taskrunner.md` §5（HTTP API/受理语义——`POST /v1/tasks` 行）、§6（job_runs 状态机、日志分工与「日志级别约定」条目）、§10（实施计划 + 后置项总表 + 运行时边界口径）、§12（变更记录；实施时标注取代 2026-09-03「先入队后落库」决策）（评审 F16/F27/F28：原引用 §4/§5 与「风险窗口表」有偏差，已按实际章节结构修正；评审 F55：行号引用易随并发编辑漂移，以条目名为锚，数字仅作当前定位参考）
- `docs/ADR-002-asynq-async-task-executor.md`（Asynq 执行器定位；本 ADR 是其「先落事实、事务提交后再 Enqueue」铁律在 taskrunner 的落实形态——记录先行）
- 取代原草案 `.trae/documents/simplified-transactional-outbox.md`（已移除，要点并入本 ADR）
