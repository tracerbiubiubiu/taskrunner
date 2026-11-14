# 版本升级记录（Phase6 开工前）

Go 1.26.10 → 1.27.4；builder 为 golang:1.27.4-alpine3.24，runtime 为 alpine:3.24.4。PG15 → 18.6-trixie，Redis6.2/7 → 8.2.11-alpine3.22（Extended 线）；CI、Compose、E2E 使用同一目标，PG 备份镜像同版本。

本记录对应 2026-11-12 的独立维护候选，业务能力、API、运行时配置字段与数据库 schema 不变；保留 mock 接口，不启动 Phase6 业务功能。本地验证与后续 PR/部署分开记账。

GOFLAGS/GOMAXPROCS两个可选构建参数仅作用于builder，不进入运行层。构建器、运行镜像和 PG/Redis 均固定完整版本与 OCI 多架构摘要；后续更新需重新核官方摘要并跑相应门禁。CI 使用 checkout v7.0.1 / setup-go v7.0.0 的提交 SHA（Node24，托管 ubuntu-latest）；自托管 runner 至少 2.327.1。当前 checkout 的源代码以 govulncheck v1.9.0 扫描；服务另构建当前二进制扫描，不引用其它仓未推送提交。漏洞库会更新，扫描结论仅对当次环境有效；源码可达性与二进制符号扫描边界不同。

依赖按用途逐项更新：pgx 5.10→5.13 修复畸形响应 panic、simple-protocol batch 注入及不安全重试；go-redis 9.22→9.23 保持 Redis8 接入；SQLite 1.58→1.62 修复锁等待、VFS与预编译语句问题，libc 必须同步 1.80.1。crypto/sys/protobuf 及 MVS 必需的间接依赖同步安全/兼容版本。Gin1.12、Asynq0.26、Viper1.21 等已为选定稳定版本，未做无关大版本替换。go-redis9.23新增 pipeline pool，未擅自改变业务池配置，需真Redis与Asynq E2E验证。

E2E 创建随机专属 Redis 容器，只清理本次成功创建的实例，绑定回环6399；端口冲突直接失败，不先删除固定名字容器。PG/Redis新卷与存量数据迁移、暂停受理/worker及对账限制见 [部署](deployment.md)。新空卷不能作为已完成迁移的证据。

公共库仍使用原真实发布 pin（v0.4.2），通过 MVS 升级其底层 pgx/Redis 等；不写本机 replace 或不可下载提交。未来升级 utils 本身应有独立消费方验证。

官方依据：[Go发布](https://go.dev/dl/)、[pgx变更](https://github.com/jackc/pgx/blob/v5.13.0/CHANGELOG.md)、[go-redis9.23](https://github.com/redis/go-redis/releases/tag/v9.23.0)、[SQLite1.62](https://gitlab.com/cznic/sqlite/-/blob/v1.62.0/CHANGELOG.md)、[PG18镜像说明](https://hub.docker.com/_/postgres)。

仓内装配仍采用现有手工同步的 `wire_gen.go`；没有新增Wire生成器依赖，不把未配置的生成过程列为已验证。

## 验证状态

Go1.27.4 的 `make lint`、`make test`、`make build`、全包 race 均通过。无外部连接的 race 为117个测试/子测试pass事件、11个skip；随后用专属PG18.6数据库与Redis8.2.11执行同一全包race，135个pass、仅2个子进程哨兵skip，真实PG/SQLite worker、投递栅栏、panic与重放均已进入验证。6个唯一临时数据库的创建/删除都有SQL收据；专属测试库和Redis清理后，借用PG集群的数据库目录精确回到基线。

`E2E_INCLUDE_CRON=1 make e2e` 实际退出0，19项断言通过、0失败，包含分钟cron、Redis停启、reclaim、取消与崩溃冲突收敛。脚本显式指定临时SQLite，避免继承外部PG环境误连；仅清理本次Redis与匿名卷，SQLite及日志保留为审计材料。

真实Dockerfile构建linux/arm64镜像成功；提取的ELF为Go1.27.4，运行底座Alpine3.24.4，非root可写日志探针通过。当前源码及项目测试（-test）与实际ELF的govulncheck1.9默认text模式均明确输出No vulnerabilities found且退出0；JSON退出码不作为无漏洞依据。

首轮冷编译时主机负载过高、PG初始化未就绪，以及一次审计DSN的options编码错误，均保留失败收据；后续真实组合通过不删除原失败。当前完整模块图还包含未编译的compress、rsc.io/pdf目录条目，`go mod why`与生产/项目测试包图确认均未使用；已撤回的OSV另归类，不把这些目录记录写成已修运行漏洞。按G05每月补丁调查（下一次2026-12）与季度支持版本复核承接，新增调用前重新评审，不为未用驱动扩展平台。

本地日志、候选输入摘要、OCI/ELF哈希保留在本轮主仓忽略目录 `.codex/audits/version-modernization-20261112/go-siblings/`。本地只构建/运行arm64，未验证amd64应用产物；远端CI、生产部署与存量共享开发栈切换未执行，旧卷和旧实例保留。此记录不代表已发布。
