# Topic 树与 Retained 设计(`internal/topics`)

## 1. 数据结构

trie,节点 `particle`:

```go
type particle struct {
    key      string
    parent   *particle
    children map[string]*particle  // 每层独立 RWMutex
    subs     map[string]Subscription          // clientID → 订阅(含 QoS、SubIdentifier、NoLocal 等)
    shared   map[string]*sharedGroup          // group 名 → 组(原子游标 + 成员)
    retainPath string                         // 有 retained 时为 topic 全名,否则空
}
```

- **写路径**(Subscribe/Unsubscribe/Retain):全局互斥锁。订阅变更低频,可接受。
- **读路径**(每条 publish 的匹配):无全局锁,逐层 children map 读锁下探。
- 空节点 trim 在读路径之外由写锁保证,消除读/删竞态窗口。

## 2. 匹配算法

`Subscribers(topic)` DFS:每层同时下探"精确 key"与 `+` 两分支,层末收集 `#`;处理:

- `filter/#` 匹配 `filter` 本身 [MQTT-4.7.1-2]
- `$SYS`/`$share` 开头 topic 不被顶层 `#`/`+` 匹配 [MQTT-4.7.2-1]
- 同 client 多次命中:`Merge` 取最高 QoS、合并 Subscription Identifiers
- 校验:`IsValidFilter`(# 必须独占末段、+ 独占一段)、publish topic 禁通配符 [MQTT-4.7.3-1]

## 3. 共享订阅 `$share/{group}/{filter}`

- 挂树时跳过 `$share/group` 两段,filter 部分正常入 trie,节点记录在 `shared[group]`。
- 投递:组内**原子游标 round-robin** 选一名在线成员;成员离线自动跳过;`SelectStrategy` 接口预留 sticky/hash(二期)。
- 与非共享订阅同 client 命中时去重,只投一份(QoS 取高者)。
- retained 消息不发给共享订阅。

## 4. Retained

- 独立 `map[topic]Packet` + RWMutex 存储,树上节点仅留 `retainPath` 指针。
- retain 空 payload = 删除 [MQTT-3.3.1-6]。
- `Messages(filter)` 匹配订阅时按 RetainHandling 0/1/2 语义发送 [MQTT-3.8.3-1],RetainAsPublished 决定 retain flag 透传与否。

## 5. 基准

Benchmark:Subscribe/Subscribers(扇出 1/100/10k)/Unsubscribe/Retain,作为性能回归基线进 CI。
