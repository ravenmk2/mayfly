# 管理 API 与 Dashboard 设计(`internal/admin`)

## 1. 形态

- 管理面与数据面分离:独立监听地址(默认 `:8080`),echo 框架(沿用现有骨架),`/metrics` 同端口暴露。
- Dashboard:Vue3 + Element Plus,构建产物 embed 进二进制,SPA fallback(现有 `web/` 骨架扩展)。

## 2. 管理认证

- **登录**:admin 账号存 SQL 库 `admin_user`(bcrypt);首次启动无账号时生成随机密码打印到日志(仅一次)。
- **会话**:登录返回短期 token(HS256 JWT,密钥启动随机生成/可配置)。
- **API Key**:表 `api_key(id, name, key_hash, scopes, created_at, expires_at)`,Header `X-API-Key`;scope 粒度:`read` / `write` / `admin`。
- 管理面建议绑定内网地址,文档明示。

## 3. REST API(v1,JSON)

| 方法 | 路径 | 说明 |
| --- | --- | --- |
| GET | `/api/v1/status` | 版本、uptime、计数器汇总 |
| GET | `/api/v1/connections` | 分页列表(client_id/username/addr/connected_at/keepalive) |
| GET | `/api/v1/connections/{clientID}` | 详情(订阅、inflight、配额使用) |
| DELETE | `/api/v1/connections/{clientID}` | 踢除(断开连接) |
| GET/DELETE | `/api/v1/sessions/{clientID}` | 查看/清除持久会话 |
| GET | `/api/v1/subscriptions` | 分页,按 clientID/filter 过滤 |
| GET/DELETE | `/api/v1/retained` | 列表(按 topic 过滤)/删除指定 retained |
| POST | `/api/v1/publish` | 服务端发消息(topic/payload/qos/retain,v5 properties 子集) |
| GET/POST/PUT/DELETE | `/api/v1/users[/{username}]` | MQTT 用户管理(写操作同步落库并刷新认证缓存) |
| GET/PUT | `/api/v1/auth/chains` | 认证/授权源链查询与整体更新(顺序、启停、源增删;db 源每链至多一个) |
| GET/POST/PUT/DELETE | `/api/v1/acls[/{id}]` | ACL 规则管理(写后触发编译缓存重建) |
| POST | `/api/v1/auth/refresh` | 手动刷新认证/ACL 缓存 |
| GET/POST/DELETE | `/api/v1/apikeys[/{id}]` | API Key 管理(仅 admin scope) |
| POST | `/api/v1/login` | Console 登录 |
| GET | `/api/v1/setup/status` | 是否已初始化(未初始化 Console 进安装向导) |
| POST | `/api/v1/setup/test-db` | 向导内测试 MySQL 连接 |
| POST | `/api/v1/setup` | 执行初始化(主库选择 + admin 密码);已初始化返回 410 |

约定：列表统一 `page/limit/total`;错误统一 `{code, message}`;写操作幂等(PUT/DELETE 可重试)。

## 4. Dashboard 页面

概览(连接数/消息速率/订阅数折线,轮询 status)、连接、会话、订阅、Retained、用户与 ACL 管理、测试发布、API Key 管理、登录页。只调上述 API,无特权通道。

视觉:**深浅双主题**(默认深色,顶栏切换,localStorage 持久化),主色琥珀 `#f59e0b`,扁平化(无渐变、4px 圆角),Element Plus dark 变量 + 自定义 CSS 变量层实现;侧边栏激活态为琥珀左指示条;品牌字标全大写 MAYFLY;产品名 **Console**。

## 5. 测试

API 层 httptest 表驱动;认证中间件、分页、错误格式覆盖;Dashboard 构建产物存在性测试(embed 不空)。
