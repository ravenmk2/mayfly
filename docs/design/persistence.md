# 持久化设计(`internal/store`)

## 1. 模型

**内存为权威状态**,Store 是崩溃恢复副本。写路径异步批量落盘,接受 ~100ms 崩溃窗口(已确认)。

## 2. 接口(窄接口,与 hook 事件流解耦)

```go
type Store interface {
    Sessions() SessionStore       // Put/Delete/Get/Iterate(会话:clientID、expiry、created/stopped)
    Subscriptions() SubStore      // Put/Delete/DeleteByClient/Iterate
    Inflight() InflightStore      // Put/Delete/DeleteByClient/Iterate(clientID, packetID, packet, 方向)
    Retained() RetainedStore      // Put/Delete/Iterate(topic, packet, expiry)
    Close() error
}
```

实现:`mysql`(默认)、`sqlite`(内置)、`memory`(测试/纯内存运行)。同一 SQL 实现双 driver(与 auth 共用连接池)。

## 3. 异步批量写

```go
type BatchWriter struct {
    queue chan writeOp   // 有界;满则阻塞调用方(背压)而非丢写
    // flush 触发:interval(默认 100ms) 或 batchSize(默认 500)
}
```

- 单事务批量提交;失败重试 3 次后告警计数 `store.write_errors`(不阻塞 broker)。
- 启动恢复:各 Iterate 增量加载进内存结构;加载期间 broker 先起存储再起监听器,保证恢复完成后才接受连接。
- 不实现迁移框架:内置 `schema_migrations` 表 + 顺序 SQL 语句(auto-migrate)。

## 4. SQL Schema

```sql
CREATE TABLE mf_session (
  client_id     VARCHAR(255) PRIMARY KEY,
  session_expiry INT NOT NULL,
  created_at    BIGINT NOT NULL,
  stopped_at    BIGINT NOT NULL
);
CREATE TABLE mf_subscription (
  client_id VARCHAR(255) NOT NULL,
  filter    VARCHAR(512) NOT NULL,
  qos       TINYINT NOT NULL,
  no_local  BOOLEAN NOT NULL DEFAULT FALSE,
  retain_as_published BOOLEAN NOT NULL DEFAULT FALSE,
  retain_handling TINYINT NOT NULL DEFAULT 0,
  sub_id    INT NULL,                      -- v5 subscription identifier
  PRIMARY KEY (client_id, filter)
);
CREATE TABLE mf_inflight (
  client_id VARCHAR(255) NOT NULL,
  packet_id INT NOT NULL,
  direction TINYINT NOT NULL,              -- 0=in 1=out
  packet    BLOB NOT NULL,                 -- 编码后的 packet 字节
  expiry    BIGINT NOT NULL DEFAULT 0,
  PRIMARY KEY (client_id, direction, packet_id)
);
CREATE TABLE mf_retained (
  topic  VARCHAR(512) PRIMARY KEY,
  packet BLOB NOT NULL,
  expiry BIGINT NOT NULL DEFAULT 0
);
```

## 5. 一致性与窗口

- QoS1/2 崩溃恢复:未落盘的出向 inflight 丢失 → 客户端重连/超时由 QoS 重发机制兜底;入向 QoS2 已回 PUBREC 未落盘的,PUBREL 重到时按幂等处理。
- retained 丢失窗口内的 retain 写：可接受(文档明示)。
- 顺序：同一实体的写 op 保序(单队列);不同实体间无序不影响正确性。
