# AI Agents 工作规范

Mayfly：Mayfly is a lightweight MQTT broker

## 目录结构

```txt
mayfly/
├── cmd/mayfly/   # 程序入口
├── internal/          # 应用私有代码
├── web/               # 前端源码与构建产物
└── scripts/           # 辅助脚本
```

## 文档索引

文档变化时同步更新

- docs/architecture.md：总体架构、组件、I/O 模型、关键决策清单
- docs/design/protocol.md：协议编解码层
- docs/design/session.md：会话生命周期与 QoS 状态机
- docs/design/topics.md：topic 树、共享订阅、retained
- docs/design/auth.md：认证与 ACL(MySQL/SQLite、编译缓存)
- docs/design/setup.md：安装向导、数据目录、主密钥
- docs/design/persistence.md：持久化 Store 接口与异步批量
- docs/design/listeners.md：TCP/TLS/WS/WSS 监听器
- docs/design/observability.md：$SYS、Prometheus、日志
- docs/design/admin-api.md：管理 REST API 与 Console
- docs/design/testing.md：分层测试策略
