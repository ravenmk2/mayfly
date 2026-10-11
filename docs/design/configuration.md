# 配置系统设计(`internal/config`)

## 1. 分层与优先级

高到低：CLI flag > `MAYFLY_*` 环境变量 > YAML 文件 > 内置默认。

- 文件:`--config` 指定,默认 `./data/mayfly.yaml`(docs/design/setup.md)。默认路径文件不存在 → 用内置默认并告警;显式指定的文件不存在 → 启动失败。
- 环境变量:`MAYFLY_` 前缀 + 下划线分隔的键路径,贪心匹配多词键:`MAYFLY_LOG_LEVEL` → `log.level`,`MAYFLY_DATA_DIR` → `data_dir`,`MAYFLY_BROKER_MAX_PACKET_SIZE` / `MAYFLY_BROKER__MAX_PACKET_SIZE` 均可。数组(listeners、auth 链)不支持 env 覆盖。无法映射的 `MAYFLY_*` 变量报错(防手误);`MAYFLY_SETUP_*`(安装向导)与 `MAYFLY_TEST_*`(测试)保留跳过。
- CLI flag:`--config`、`--data-dir`、`--version`。
- 配置热更新不做,重启生效(架构 A10 一期范围)。

## 2. 结构

```yaml
data_dir: ./data
listeners:
  - type: tcp            # tcp | tls | ws | wss
    id: tcp              # 可省,默认取 type;多实例时需唯一
    address: ":1883"
    max_connections: 0   # 0 = 不限;全局上限另见 broker.max_connections
  - type: tls
    address: ":8883"
    tls:
      cert_file: ./data/server.crt
      key_file: ./data/server.key
      ca_file: ./data/ca.crt                # client_auth 为 require_and_verify 时必填
      client_auth: require_and_verify       # none(默认)| require_and_verify(mTLS)
      use_cn_as_username: true              # 证书 CN 作认证用户名,需 mTLS
  - type: ws
    address: ":8083"
    path: /mqtt                             # 默认 /mqtt,其他路径 404
    allowed_origins: []                     # 默认同源;"*" 全放行
  - type: wss
    address: ":8084"
    tls: { cert_file: ./data/server.crt, key_file: ./data/server.key }
broker:                   # 过载保护配额(架构 §5),0 = 内置默认
  max_connections: 0
  max_packet_size: 1048576
  receive_maximum: 64
  topic_alias_maximum: 16
  outbound_queue_depth: 1024
store:
  dsn: ""                # sqlite:<path> | mysql:<driver-dsn>;空 = 仅内存
auth:                    # 对齐 auth.Manager 的 Config(docs/design/auth.md §3)
  anonymous_allow: false
  refresh_interval: 5s
  authentication:
    - { type: db, dsn: "sqlite:./data/mayfly.db" }        # enabled 省略即 true
    - { type: file, path: ./data/users.yaml, enabled: false }
  authorization:
    - { type: db, dsn: "sqlite:./data/mayfly.db" }
log:
  level: info            # debug | info | warn | error
  format: text           # text | json
  output: stderr         # stderr | stdout | 文件路径
admin:
  addr: ":8080"
```

## 3. 校验

加载即校验,错误汇总并带字段路径:端口范围(0-65535)、监听器类型与 id 唯一性、TLS 证书/密钥/CA 文件存在性、`client_auth` 取值与 `use_cn_as_username` 前置依赖、ws path 以 `/` 开头、`max_packet_size` 不超过协议上限 256MiB、store DSN scheme、auth 链 file/db 必填项与 db 源唯一性(每条链至多一个启用的 db 源)、log level/format。

## 4. 缺省行为

无配置文件:tcp :1883 + admin :8080、仅内存存储、无认证源(allow-all 并告警)——与早期 milestone 行为兼容。
