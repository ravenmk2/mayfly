# Mayfly 架构设计

> 版本:v0.1(2026-10-12 需求确认后定稿)

Mayfly 是轻量级 MQTT Broker，对标主流单机 broker 的核心能力，MQTT 3.1.1 与 5.0 完整支持。

## 1. 定位与约束

| 维度 | 决策 |
| --- | --- |
| 协议 | MQTT 3.1.1 + 5.0 完整支持(单 Packet 结构，双版本编解码) |
| 部署形态 | 单节点优先；Router/Store 抽接口，为集群预留 |
| 性能目标 | 万级连接 / 数万 msg/s;I/O 模型采用每连接 2 goroutine |
| 交付 | 独立二进制(内嵌 Dashboard)+ Docker;不作为 library 发布，全部代码在 `internal/` |
| QoS 承诺 | QoS 0/1/2 全支持；**入向 QoS2 严格 exactly-once**(等 PUBREL 才投递) |

## 2. 组件总览

```
                ┌──────────────────────────────────────────────┐
                │                  cmd/mayfly                   │
                └───────────────────┬──────────────────────────┘
                                    │
   TCP/TLS/WS/WSS ──► listener ──► broker(Server) ◄──── config(YAML+env)
                                    │
        ┌───────────────┬───────────┼───────────────┬──────────────┐
        ▼               ▼           ▼               ▼              ▼
     packet         session       topics          auth         metrics
   (编解码/校验)  (会话/QoS/inflight) (trie/共享/retained) (认证+ACL 缓存) ($SYS/Prometheus)
                        │           │               │
                        └───────────┴──────┬────────┘
                                           ▼
                                  store(Store 接口)
                            ┌──────┴───────┬────────────┐
                          MySQL         SQLite        内存(测试)
                                           ▲
                          hook(接口分组+类型断言,贯穿各组件事件点)
                                           ▲
                 admin(REST API + Dashboard embed + API Key)
```

包布局(全部在 `internal/`,不作为公开 API):

| 包 | 职责 |
| --- | --- |
| `internal/packet` | MQTT 3.1.1/5.0 编解码、校验、reason code 表、mempool |
| `internal/topics` | 订阅 trie、通配符匹配、共享订阅 round-robin、retained 索引 |
| `internal/session` | 会话生命周期、inflight、PacketID 分配、QoS 状态机、配额 |
| `internal/broker` | Server 装配、连接生命周期、投递引擎、$SYS 数据源 |
| `internal/listener` | TCP/TLS/WS/WSS → `net.Conn` 归一 |
| `internal/auth` | 认证/授权接口、SQL 实现(MySQL/SQLite)、ACL 编译缓存 |
| `internal/store` | Store 接口、异步批量落盘、启动恢复、MySQL/SQLite/内存实现 |
| `internal/hook` | 接口分组注册表(AuthHook/AuthzHook/SessionHook/MessageHook/MetricsHook) |
| `internal/metrics` | 原子计数器、$SYS 发布器、Prometheus `/metrics` |
| `internal/admin` | REST API、Dashboard SPA、admin 账号 + API Key 认证 |
| `internal/config` | YAML 加载 + `MAYFLY_*` 环境变量覆盖 |

## 3. I/O 模型与写路径

每连接 2 goroutine + 1 个全局 housekeeping goroutine:

- **读 goroutine**:阻塞读 → packet 解码 → 校验 → 状态机处理。**任何包都不在此直接写 socket**。
- **写 goroutine**:消费该连接唯一的有界 outbound 队列,聚合刷写。
  - 关键设计要点:**控制包与 PUBLISH 统一进同一个 outbound 队列**,天然保序,避免"控制包直写 + publish 走 channel"双通道竞态与慢客户端阻塞读循环。
  - 队列满:QoS0 丢弃并计 `messages.dropped`;QoS1/2 已在 inflight 中,等 PUBACK 后由 `NextImmediate` 补发,不丢。
- **housekeeping**:ticker 驱动 $SYS 发布(10s)、会话/inflight/retained 到期检查。所有到期结构用**最小堆**组织,避免每秒 O(n) 全表扫描。

## 4. 关键架构决策

| # | 决策 | 理由 |
| --- | --- | --- |
| A1 | 单 `Packet` struct 承载 15 种包类型,每类型 Encode/Decode/Validate | 无反射、代码密度高；代码内标注 `[MQTT-x.x.x-x]` 规范条目号 |
| A2 | 读/写 goroutine 分离 + **单写队列** | 保序、背压集中、读循环永不被对端慢拖累 |
| A3 | topic 树读路径无全局锁(逐层 RWMutex),写路径全局锁 | 订阅变更是低频操作,publish 匹配是热路径 |
| A4 | 共享订阅 per-group 原子游标 round-robin | 避免伪随机选人；策略接口预留 sticky/hash |
| A5 | ACL:订阅时校验 subscribe 权限,发布时校验 publish 权限,**投递时不再逐订阅者复查** | 消除投递热路径的 ACL 成本；ACL 变更对存量订阅的回收靠缓存刷新+会话重订阅,文档明示 |
| A6 | 内存为权威状态,Store 异步批量落盘(默认 100ms/批量) | 接受 ~100ms 崩溃窗口换吞吐；启动增量恢复 |
| A7 | 认证/授权分离，各自为多源链式 fall-through(有序链式)；一期 file+db 源，http 二期;MQTT 用户与 Console 用户分表 | 弱源不能绕过强源；设备凭据与管理权限隔离；外接认证预留 |
| A8 | hook 用接口分组 + 类型断言,不用大接口 + Provides 枚举 | 编译期安全,无样板 |
| A9 | 过期清理一律最小堆 | 避免全表扫描 |
| A10 | 不实现集群/桥接/规则引擎(二期);Router 接口隔离投递引擎 | 轻量定位;规则引擎仅预留消息钩子 |
| A11 | 首次运行安装向导选主库(SQLite/MySQL)并设 admin 密码、生成主密钥;一切可变数据落 `./data`(相对 cwd) | 开箱即用且零硬编码密钥;详见 design/setup.md |

## 5. 过载保护配额

- `max_connections`(listener 级/全局)
- `max_packet_size`(默认 1MB,可配)
- 每客户端:inflight 收/发窗口(v5 Receive Maximum 双向)、outbound 队列深度、topic alias 上限
- 超限行为：连接即拒(v5 用 reason code 0x97 等)、丢 QoS0 并计数、断开恶性客户端
- 速率限制(msg/s、字节/s)**二期**做 token bucket

## 6. 持久化概要(详见 design/persistence.md)

实体:Session、Subscription、Inflight 消息、Retained 消息 → `Store` 窄接口 + 写路径异步批量(WAL 语义的写后队列,按时间/条数双触发 flush)。认证数据(users/ACL)走 `auth` 包的独立接口,同一 SQL 后端实现。启动时增量加载恢复。

## 7. 安全概要(详见 design/auth.md)

- 认证:bcrypt 密码哈希、superuser、匿名开关、连接级缓存
- ACL:规则链 + 首条命中,编译缓存(规则→分段匹配器),支持 `%u`/`%c` 变量,默认拒绝,5s 定时刷新 + API 手动刷新
- MySQL 故障:缓存继续服务存量连接,新连接认证失败即拒
- 管理面:admin 账号(首启生成随机默认密码打印到日志)+ API Key(多把、带 scope)

## 8. 可观测概要(详见 design/observability.md)

- `$SYS/broker/...`:retained 发布,10s 周期(不做每秒 ReadMemStats)
- Prometheus `/metrics`:连接/消息/字节/订阅/inflight/认证失败/丢弃等计数
- 日志:`slog`,开发 text / 生产 JSON,级别与输出可配

## 9. 明确不做(一期)

集群、MQTT 桥接、规则引擎(仅预留接口)、速率限制、配置热更新(重启生效)、WebSocket 调试客户端以外的花哨 Dashboard 功能。
