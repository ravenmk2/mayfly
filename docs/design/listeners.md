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
| `tls` | 8883 | TLS,cert/key 配置;`client_auth: require_and_verify` 开 mTLS,证书 CN 可作为 username 认证源 |
| `ws` | 8083 | WebSocket(gorilla/websocket),BinaryMessage 流适配为 net.Conn,Path 可配(默认 `/mqtt`) |
| `wss` | 8084 | WS + TLS |

- TLS 不作为独立 listener 类型,是配置项(`tls.cert_file/key_file/client_auth`)。
- 每监听器可配 `max_connections`;全局另有上限。
- 优雅关闭：先停 Accept,再等待连接 goroutine(带超时强杀)。

## 3. 读缓冲

每连接 `bufio.Reader`(默认 4KB 可配);`max_packet_size` 在 fixed header 解码后即检查,超限直接断开,不读完整个包。
