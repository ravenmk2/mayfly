# 会话与 QoS 状态机设计(`internal/session`)

## 1. Client 结构

```go
type Client struct {
    ID       string
    Props    ClientProperties // v5 properties、Will、Username、ProtocolVersion、CleanStart
    State    ClientState      // Inflight 入/出、Subscriptions、TopicAliases、Keepalive、SessionExpiry
    Net      ConnContext      // net.Conn + bufio + ListenerID
    Outbound chan Packet      // 唯一写队列(见 architecture A2)
    takenOver atomic.Bool
    sync.RWMutex
}
```

## 2. 会话生命周期

1. CONNECT → 认证 → **会话接管**:同 ID 老连接发 `ErrSessionTakenOver` 断开,老会话非 clean 则迁移订阅与 inflight 到新 Client(`takenOver` 标记防止老连接 defer 清理误删新会话)。
2. v3:`Clean` 决定断开是否清会话;v5:`Clean Start` + `Session Expiry Interval`(0=断开即清,0xFFFFFFFF=永不过期)。CONNACK 带 `Session Present`。
3. 持久会话断开保留,由最小堆按 `StopTime+expiry` 到期清理。
4. 重连且 session present:重发未完成 inflight(DUP=1)。

## 3. Inflight 与流控

- 出向:`map[uint16]*InflightMessage{Packet, Expiry, Sent}` + receive maximum quota 计数;配额满则排队(不置 Sent),PUBACK/PUBCOMP 释放后立即补发。
- 入向:发布者 → broker 的 QoS1/2 同样受对端 Receive Maximum 约束,超限回 DISCONNECT(0x93 Receive Maximum Exceeded)。
- **到期用最小堆**(按 Expiry),不做全表扫描。
- PacketID 分配：游标 + 位图,O(1) 摊还;不再线性扫描。

## 4. QoS 流程

**入向(发布者→broker)**
- QoS0:直接进投递引擎。
- QoS1:进投递引擎 → 回 PUBACK(v5 可带 reason code)。
- QoS2:**严格 exactly-once**——收到 PUBLISH 存 inflight 并回 PUBREC,**不投递**;收到 PUBREL 才进投递引擎,回 PUBCOMP 并删 inflight。重复 PUBLISH(DUP)只重发 PUBREC;重复 PUBREL 只重发 PUBCOMP。

**出向(broker→订阅者)**
- QoS0:出站队列满即丢弃计数。
- QoS1:存 inflight → 发送;收 PUBACK 删。重连 DUP 重发。
- QoS2:存 inflight → 发送;收 PUBREC 转存 PUBREL 态;收 PUBCOMP 删。各阶段重发按规范 DUP 规则。

**降级**:投递 QoS = min(发布 QoS, 订阅 QoS) [MQTT-3.8.4-3]。

## 5. 其他机制

- **Keepalive**:读 goroutine 用 conn.SetReadDeadline(1.5×keepalive);0 = 不检测 [MQTT-3.1.2-24]。
- **Will**:断连非正常(非 DISCONNECT 或 v5 reason≠0x00)时发布;Will Delay 由最小堆调度,会话恢复取消。
- **Topic Alias**:入向 alias→topic 映射,出向反向映射 + cursor;0 或超上限断开(0x94)。
- **Message Expiry**:inflight/retained 统一过期堆;投递时重写剩余秒数。
