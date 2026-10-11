# 可观测性设计(`internal/metrics`)

## 1. 计数器

全部 atomic int64 计数(热路径零锁):

连接(online/total)、消息(received/sent/dropped,按 QoS)、字节(收/发)、订阅数、retained 数、inflight 数、认证成功/失败、ACL allow/deny、store 写错误、goroutine 数、堆内存(低频采样)。

## 2. $SYS

- 10s 周期(可配),以 **retained** 消息发布 `$SYS/broker/...`:uptime、version、clients.connected/total、messages.received/sent/dropped、subscriptions.count、retained.count、bytes.received/sent、load.bytes 等。
- 复用正常 retain+publish 路径;不做每秒 `runtime.ReadMemStats`(10s 周期采样)。
- `$SYS` 不被顶层通配符匹配 [MQTT-4.7.2-1]。

## 3. Prometheus

`/metrics`(管理端口),`prometheus/client_golang` 输出:

- `mayfly_connections`、`mayfly_connections_total`、`mayfly_messages_received_total{qos}`、`mayfly_messages_sent_total{qos}`、`mayfly_messages_dropped_total`、`mayfly_messages_oversized_total`、`mayfly_bytes_total{direction}`、`mayfly_subscriptions`、`mayfly_retained`、`mayfly_inflight`、`mayfly_auth_results_total{result}`、`mayfly_auth_source_results_total{source,chain,result}`(有认证管理器时)、`mayfly_acl_results_total{result}`、`mayfly_store_write_errors_total`、`mayfly_goroutines`、`mayfly_heap_alloc_bytes`、`mayfly_uptime_seconds`、`mayfly_build_info{version}`

## 4. 日志

`slog`:`--log.format=text|json`(默认 text)、`--log.level`、`--log.output=stdout|文件路径`(文件输出配 lumberjack 轮转)。
关键事件必带结构化字段:connect(client_id/username/remote_addr)、auth failure(原因,不含密码)、kick、session takeover、store 错误。消息收发不打日志(热路径)。
