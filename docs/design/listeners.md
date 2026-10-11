# 监听器设计(`internal/listener`)

## 1. 抽象

向 `net.Conn` 归一:

```go
type Listener interface {
    Serve(ctx context.Context, establish EstablishFunc) error
    Close() error
    ID() string
    Addr() string
}
type EstablishFunc func(id string, c net.Conn) error
```

## 2. 类型

| ID | 默认端口 | 说明 |
| --- | --- | --- |
| `tcp` | 1883 | 明文 |
| `tls` | 8883 | TLS,cert/key 配置;`client_auth: require_and_verify` 开 mTLS,`use_cn_as_username` 将证书 CN 作为 username 认证源(经 `Conn.CertUsername` 传给 broker) |
| `ws` | 8083 | WebSocket(gorilla/websocket),BinaryMessage 流适配为 net.Conn,Path 可配(默认 `/mqtt`) |
| `wss` | 8084 | WS + TLS(同一份 tls 配置块,支持 mTLS 与 CN 转发) |

- 配置的 listeners 数组有四个类型;`wss` 复用 `ws` 实现叠加 tls 块,TLS 版本下限 1.2。
- WS 适配:读拼接 Binary 帧、写整帧消息、Close 先发 CloseMessage;非 Binary 帧按 [MQTT-6.0.0-1] 断开(close 1003)。CheckOrigin 默认拒绝跨域(无 Origin 头的非浏览器客户端放行),`allowed_origins` 可配,`*` 全放行。
- 每监听器可配 `max_connections`;全局另有上限。
- 优雅关闭：先停 Accept,再等待连接 goroutine(带超时强杀)。

## 3. 读缓冲

每连接 `bufio.Reader`(默认 4KB 可配);`max_packet_size` 在 fixed header 解码后即检查,超限直接断开,不读完整个包。
