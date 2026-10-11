# 认证与授权设计(`internal/auth`)

## 1. 原则

- **认证(AuthN)与授权(AuthZ)分离**,各自是**多源链式 fall-through***REMOVED***
- 用户体系分离:**MQTT 用户**(`mqtt_user`,设备凭据,数量级万级)与 **Console 用户**(`admin_user`,个位数)+ `api_key` 完全分表,MQTT 认证路径永远碰不到管理权限。
- 一期实现 `file`(JSON/YAML/TOML)+ `db`(MySQL/SQLite)两种源;`http` 源二期(链接口已兼容)。
- 链在 Console 可视化配置:添加/删除/启停/上下调序即时生效(原子替换);**db 源(内置数据库,安装时选定 SQLite/MySQL)每条链至多一个,不可删除只可禁用**;file 源可有多个。
- db 源:MySQL 与 SQLite 共用一套 SQL 逻辑,driver 可换;SQLite 用纯 Go driver(无 CGO)。

## 2. 链语义

```
认证链(按配置顺序):
  source.authenticate(username, password):
    找到且密码正确   → allow,链停止
    找到但密码错误   → deny,链停止        # 防止用弱源绕过强源
    未找到该用户     → ignore → fall through
  全部 ignore       → deny;若开启匿名开关且无密码字段 → 匿名放行(不授予 superuser)

授权链(按配置顺序):
  source.authorize(client, op, topic):    # op: publish | subscribe
    规则命中 → allow / deny,链停止        # 源内按 seq 首条命中,源间按链序
    无命中   → ignore → fall through
  全部 ignore → 默认拒绝
```

superuser 跳过整条授权链。认证结果随连接缓存(连接存续期有效)。

## 3. 源配置示例

```yaml
auth:
  anonymous_allow: false
  authentication:                    # 认证链,按序 fall-through
    - type: db                       # db 源
      backend: mysql                 # mysql | sqlite
    - type: file                     # file 源
      path: /etc/mayfly/users.yaml
  authorization:                     # 授权链
    - type: db
      backend: mysql
    - type: file
      path: /etc/mayfly/acl.yaml
```

file 源(users.yaml / acl.yaml):

```yaml
# users.yaml
users:
  sensor1: { password: "$2y$10$...(bcrypt)", superuser: false }

# acl.yaml("*" = 所有用户;也可写 clientid = "...")
rules:
  - { username: "admin",   action: allow, permission: pubsub,    topic: "#" }
  - { username: "*",       action: deny,  permission: pubsub,    topic: "$SYS/#" }
  - { username: "*",       action: allow, permission: publish,   topic: "up/{user}" }
  - { username: "*",       action: allow, permission: subscribe, topic: "down/{user}" }
  - { username: "*",       action: allow, permission: subscribe, topic: "broadcast/#" }
  - { username: "sensor1", action: allow, permission: pubsub,    topic: "devices/dev1/#" }
```

## 4. SQL Schema(MySQL/SQLite 通用方言子集)

```sql
CREATE TABLE mqtt_user (
  username      VARCHAR(255) PRIMARY KEY,
  password_hash VARCHAR(255) NOT NULL,   -- bcrypt
  superuser     BOOLEAN NOT NULL DEFAULT FALSE,
  created_at    TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP
);

CREATE TABLE mqtt_acl (
  id          BIGINT AUTO_INCREMENT PRIMARY KEY,  -- SQLite: INTEGER PRIMARY KEY AUTOINCREMENT
  seq         INT NOT NULL,                        -- 源内顺序(首条命中依据)
  username    VARCHAR(255) NULL,                   -- NULL 且 clientid NULL = 全局规则
  clientid    VARCHAR(255) NULL,
  action      VARCHAR(8)  NOT NULL,                -- allow | deny
  permission  VARCHAR(8)  NOT NULL,                -- publish | subscribe | pubsub
  topic       VARCHAR(512) NOT NULL                -- filter,支持 + # 与 {user}/{clientid} 占位符
);
CREATE INDEX idx_acl_user ON mqtt_acl(username);
```

Console 侧 `admin_user` / `api_key` 表见 design/admin-api.md。

## 5. ACL 效率设计(目标场景:每客户端一用户 + 上行/下行两个专属 topic + 少量广播 topic)

### 5.1 三层机制

1. **模板规则消灭 N 倍规则数**:上行/下行 topic 有规律时(如 `up/{user}`、`down/{user}`),只需 2 条全局模板规则 + 广播规则,总规则数 O(1),与设备数无关。占位符段在编译期标记为 `varUser/varClient`,匹配时按段与 username/clientid 直接比较,不做字符串拼接。占位符语法为 `{user}`(当前 username)与 `{clientid}`(当前 clientid),独占一个 topic 段。
2. **主体分桶索引**:无规律的 per-user 专属规则按 `map[username][]rule` / `map[clientid][]rule` 分桶,全局规则单独一桶。判定时候选 = 该用户桶 + 该 clientid 桶 + 全局桶,按 seq 归并取首条命中。复杂度 O(个人规则数 + 全局规则数) ≈ 常数。
3. **决策缓存**:`map[(username, op, topic)] → decision`,附 generation 计数;规则刷新时 generation 递增,旧缓存整体失效。稳定运行下热路径 ≈ 一次 map 查找。

规则编译产物:

```go
type compiledRule struct {
    seq        int
    action     Action              // allow | deny
    permission Permission          // publish | subscribe | pubsub
    segments   []segment           // 字面量 | '+' | '#' | varUser | varClient
}
type ruleSet struct {              // 每个授权源一份
    global   []compiledRule
    byUser   map[string][]compiledRule
    byClient map[string][]compiledRule
    version  uint64                // 决策缓存的 generation
}
```

### 5.2 缓存与刷新

- file/db 源：后台 5s 定时重载 + 管理 API 手动触发；整体构建新 ruleSet 后**原子替换**(RCU),读者无锁。
- 决策缓存随 ruleSet version 失效，与规则刷新联动。
- http 源（二期）：无索引，短期 TTL 缓存（默认 5s)。

### 5.3 变更不回溯

规则变更只影响新判定；**已建立的订阅不回溯回收**，客户端重订阅时按新规则校***REMOVED***

## 6. 故障行为

- db 源不可达：内存中最后一份 ruleSet 继续服务存量判定；新连接认证失败即拒（fail-closed),v5 reason 0x86/0x87；无旧缓存时默认拒绝。
- 认证失败、授权拒绝均计数：`mayfly_auth_results_total{source,result}`。

## 7. 密码

bcrypt(cost 默认 10);CLI `mayfly passwd <user>` 与 admin API 均可增改；密码永不出现在日志(含 debug)。
