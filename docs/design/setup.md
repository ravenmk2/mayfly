# 安装向导与数据目录设计

## 1. 数据目录

一切可变数据保存在 `./data`(相对进程工作目录,可用 `--data-dir` / `MAYFLY_DATA_DIR` 覆盖):

```
./data/
├── mayfly.yaml      # 主配置(向导生成,可手改;YAML + MAYFLY_* env 覆盖)
├── mayfly.db        # SQLite 主库(主数据库选 SQLite 时)
├── master.key       # 主密钥,32 字节随机,文件权限 0600
└── logs/            # 日志输出为文件时的默认位置
```

## 2. 首次运行检测

`./data/mayfly.yaml` 不存在 → 未初始化:
- MQTT 监听器**不启动**(不接受设备连接);
- 管理监听启动,Console 任何页面重定向到安装向导;API 仅放行 `/api/v1/setup/*`,其余返回 428/重定向。

## 3. 向导流程(Console)

1. **主数据库选择**:SQLite(内置,零配置,推荐默认)/ MySQL(填 host/port/账号/密码/库名 + 测试连接,通过才能下一步)。该选择即 auth/store 的 db 后端(内置数据库),写入配置。
2. **管理员密码**:创建 Console 管理员 `admin`(bcrypt 存 `admin_user` 表;与 MQTT 用户体系隔离)。
3. **主密钥生成**:自动生成 32 字节随机密钥,写 `./data/master.key`(0600),页面提示备份。

完成后写 `./data/mayfly.yaml`,初始化 schema(auto-migrate),进入登录页。

> 当前生成的 mayfly.yaml 以明文保存 DSN(含 MySQL 密码);`enc:` 加密存储(见第 4 节)尚未实现,后续里程碑补齐。

## 4. 主密钥用途

- Console 会话 JWT(HS256)签名密钥;
- 配置文件中敏感项的对称加密(AES-256-GCM),如 MySQL 密码在 `mayfly.yaml` 中以 `enc:...` 形式存储;
- 丢失后果:JWT 全部失效(重新登录即可)+ 加密配置项不可解密(需重配)。不用于 MQTT 用户密码(bcrypt 单向哈希,与主密钥无关)。

## 5. 非交互初始化(自动化部署)

环境变量直通:`MAYFLY_SETUP_DB=sqlite|mysql`、`MAYFLY_SETUP_ADMIN_PASSWORD=...`、`MAYFLY_SETUP_MYSQL_DSN=...` 齐全时跳过向导直接初始化(主密钥自动生成),便于容器/脚本化部署。

## 6. API

| 方法 | 路径 | 说明 |
| --- | --- | --- |
| GET | `/api/v1/setup/status` | `{initialized: bool}`;initialized = 配置文件存在且 `admin_user` 非空,未初始化时 Console 据此进向导 |
| POST | `/api/v1/setup/test-db` | 向导内测试 MySQL 连接(不持久化) |
| POST | `/api/v1/setup` | 执行初始化(db 选择、admin 密码);已初始化返回 410 Gone。成功响应 `{initialized: true, restart_required: true}`:管理面即刻解锁(签发 JWT 的主密钥已切换为落盘密钥),MQTT 监听器需重启进程后启动 |
