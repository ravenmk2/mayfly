# 协议层设计(`internal/packet`)

## 1. 范围

MQTT 3.1.1(protocol level 4)与 5.0(level 5)双协议栈，接受 3.1(level 3,"MQIsdp")按 3.1.1 处理。15 种控制包全支持:CONNECT/CONNACK/PUBLISH/PUBACK/PUBREC/PUBREL/PUBCOMP/SUBSCRIBE/SUBACK/UNSUBSCRIBE/UNSUBACK/PINGREQ/PINGRESP/DISCONNECT/AUTH(v5)。

## 2. 核心结构

单一 `Packet` struct 承载所有包类型(不用接口多态):

```go
type Packet struct {
    FixedHeader FixedHeader      // Type/Qos/Dup/Retain/Remaining
    ProtocolVersion byte         // 编解码按版本分支(properties 仅 v5)
    PacketID   uint16
    TopicName  string
    Payload    []byte
    Filters    []Subscription    // SUBSCRIBE/UNSUBSCRIBE
    ReasonCodes []byte
    Properties Properties        // v5 flat struct,带 XxxFlag 区分 0 值/未设置
    Connect    ConnectParams     // Keepalive/CleanStart/Will/Username/Password...
    Mods       Mods              // 编码控制(MaxSize 等)
    // broker 内部字段:Origin/Created/Expiry 等,不参与编码
}
```

每种包类型一对方法 `ConnectEncode/ConnectDecode` ... + `Validate()`。错误统一为 `Code{Code byte, Reason string}`(实现 error),全量 v5 reason code 定义在 `codes.go`,附 `V5ToV3` 降级映射表。

## 3. 编解码要点

- `FixedHeader` 解码严格校验 flag 位 [MQTT-2.1.3-1](如 PUBREL 必须 0010,SUBSCRIBE 必须 0010)
- 可变长度整数 1–4 字节,`Remaining` 上限 268435455 [MQTT-2.1.4-1]
- UTF-8 字符串校验:合法 UTF-8、无 NUL、无通配符(按场景)
- Properties:flat struct + `validPacketProperties` 表做"属性×包类型"合法性校验;编码用 `sync.Pool` 的 buffer 池(mempool)减分配
- 解码安全:`bytesToString` 用安全拷贝,不用 unsafe 零拷贝(避免 payload 别名引发的隐蔽 bug)
- 所有规范强约束点在代码中以 `[MQTT-x.x.x-x]` 注释标注

## 4. v5 语义覆盖(全量)

| 特性 | 处理 |
| --- | --- |
| Session Expiry Interval | session 生命周期,见 design/session.md |
| Message Expiry Interval | retained/inflight 均参与过期,投递时重算剩余值 |
| Receive Maximum | 入/出双向 inflight 配额 |
| Topic Alias | 入向 map[uint16]string、出向 map[string]uint16+cursor,上限可配 [MQTT-3.3.2-11] |
| Subscription Identifier | 订阅时记录,投递时在 PUBLISH properties 携带 |
| Will Delay Interval | housekeeping 延迟发送,会话恢复则取消 |
| User Properties | 透传 |
| Response Topic / Correlation Data | 透传(request-response 模式由客户端实现) |
| Server Reference / Enhanced Auth(AUTH 包) | 解析并拒绝(0x9A/0x9C 类响应),不实现增强认证 |
| Maximum Packet Size | 双向协商,超限断开/回错 |

## 5. 测试(黄金字节表)

`tpackets_test.go` 风格的 `TPacketCase{RawBytes, Packet, Expect}` 表驱动:每个包类型覆盖合法样本 + 畸形样本(坏 flag、截断、属性越界、长度不符)。编解码 round-trip 一致性。解码器配 fuzz(`FuzzDecode`)。
