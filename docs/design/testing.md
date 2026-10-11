# 测试策略

## 1. 分层

| 层 | 手段 | 覆盖目标 |
| --- | --- | --- |
| 协议编解码 | 黄金字节表(table-driven)+ round-trip + `FuzzDecode` | 15 种包 × 合法/畸形 × 3.1.1/5.0 |
| topics | 单测 + benchmark(扇出回归基线) | 通配符边界、共享订阅、retained 语义 |
| 会话/QoS | 单测(状态机)+ net.Pipe 集成 | QoS0/1/2 出入向全流程、接管、恢复、expiry |
| broker 集成 | net.Pipe 起真实 Server,字节级驱动完整协议交互 | CONNECT→SUB→PUB→断连→重连恢复等场景 |
| 互操作 | eclipse/paho.golang 作客户端库跑真实 TCP | 与主流客户端互通;CI 三平台 |
| 持久化 | 内存/SQLite 实现单测 + MySQL(testcontainers,标记 integration) | 崩溃恢复等价性 |
| 认证/ACL | SQL 后端单测(SQLite)+ 规则编译缓存单测 | 首条命中、变量、刷新原子性 |
| admin API | httptest 表驱动 | 见 design/admin-api.md |
| 端到端 | `scripts/e2e` 起二进制,mqtt 客户端冒烟 | 发布/订阅/retained/QoS2 |

## 2. 约定

- testify `require`;并发路径必跑 `-race`;CI:ubuntu/windows/macos 三平台 + lint(golangci)。
- 集成测试默认 SQLite/内存存储;MySQL 用例打 `//go:build integration`,CI 用 service container 跑。
- 互操作矩阵:paho golang(QoS0/1/2、v3.1.1/v5、retained、will、共享订阅)。
- 每个里程碑必须有可运行的验证命令。

## 3. 真机验证

发布前在真实 Linux 主机部署冒烟(部署细节见团队内部备忘,不入库):systemd 运行、e2e 冒烟脚本、连续 24h 稳定性观察(内存/连接泄漏)。
