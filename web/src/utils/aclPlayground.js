// ACL 规则演练匹配器:与运行时授权链同语义——按顺序逐条评估,
// 首条命中的规则决定 allow/deny,全部 miss 默认拒绝。
// 纯函数实现,供视图与单元测试共用。

// matchSegment 比对 filter 的一段与 topic 的一段;{user}/{clientid}
// 为占位符,+ 匹配任意单级。
export function matchSegment(filterSeg, topicSeg, username, clientid) {
  if (filterSeg === '+') return true;
  if (filterSeg === '{user}') return username !== '' && topicSeg === username;
  if (filterSeg === '{clientid}') return clientid !== '' && topicSeg === clientid;
  return filterSeg === topicSeg;
}

// topicEval 逐段比对 filter 与 topic,返回 { segs, match };
// segs 记录每一段的比对轨迹供界面高亮。
export function topicEval(filter, topic, username, clientid) {
  const fs = String(filter).split('/');
  const ts = String(topic).split('/');
  const segs = [];
  for (let i = 0; i < fs.length; i++) {
    if (fs[i] === '#') {
      segs.push({ label: '# ⇒ 剩余全部', ok: true });
      return { segs, match: true };
    }
    if (i >= ts.length) {
      segs.push({ label: fs[i] + ' ≠ ∅(topic 过短)', ok: false });
      return { segs, match: false };
    }
    const ok = matchSegment(fs[i], ts[i], username, clientid);
    let label;
    if (!ok) label = fs[i] + ' ≠ ' + ts[i];
    else if (fs[i] === '{user}' || fs[i] === '{clientid}' || fs[i] === '+') label = fs[i] + ' ⇒ ' + ts[i];
    else label = fs[i];
    segs.push({ label, ok });
    if (!ok) return { segs, match: false };
  }
  if (ts.length > fs.length) {
    segs.push({ label: '∅ ≠ ' + ts.slice(fs.length).join('/') + '(topic 多出层级)', ok: false });
    return { segs, match: false };
  }
  return { segs, match: true };
}

// evalRules 按链式首条命中语义评估规则表。rule 形如
// { seq, scope: 'user'|'client'|'all', value, action, perm, topic }。
// 返回 { rows, hit, decision }:rows 为逐条轨迹,hit 为命中规则(无则 null)。
export function evalRules(rules, { username, clientid, op, topic }) {
  const rows = [];
  let hit = null;
  for (const rule of rules) {
    if (rule.scope === 'user' && rule.value !== username) {
      rows.push({ rule, seq: rule.seq, skip: '主体不匹配(username)', segs: null, hit: false });
      continue;
    }
    if (rule.scope === 'client' && rule.value !== clientid) {
      rows.push({ rule, seq: rule.seq, skip: '主体不匹配(clientid)', segs: null, hit: false });
      continue;
    }
    if (rule.perm !== 'pubsub' && rule.perm !== op) {
      rows.push({ rule, seq: rule.seq, skip: '权限不匹配(规则限 ' + rule.perm + ')', segs: null, hit: false });
      continue;
    }
    const t = topicEval(rule.topic, topic, username, clientid);
    if (!t.match) {
      rows.push({ rule, seq: rule.seq, skip: 'topic 不匹配', segs: t.segs, hit: false });
      continue;
    }
    rows.push({ rule, seq: rule.seq, skip: '', segs: t.segs, hit: true });
    hit = rule;
    break;
  }
  return { rows, hit, decision: hit ? hit.action : 'deny' };
}

// 演练示例规则(与下方 file 源示例内容一致)。
export const playgroundRules = [
  { seq: 1, scope: 'user', value: 'admin', action: 'allow', perm: 'pubsub', topic: '#' },
  { seq: 2, scope: 'all', value: '', action: 'deny', perm: 'pubsub', topic: '$SYS/#' },
  { seq: 3, scope: 'all', value: '', action: 'allow', perm: 'publish', topic: 'up/{user}' },
  { seq: 4, scope: 'all', value: '', action: 'allow', perm: 'subscribe', topic: 'down/{user}' },
  { seq: 5, scope: 'all', value: '', action: 'allow', perm: 'subscribe', topic: 'broadcast/#' },
  { seq: 6, scope: 'user', value: 'sensor1', action: 'allow', perm: 'pubsub', topic: 'devices/dev1/#' },
];

export const playgroundExamples = [
  { topic: 'up/sensor1', op: 'publish' },
  { topic: 'down/sensor1', op: 'subscribe' },
  { topic: 'broadcast/alarm', op: 'subscribe' },
  { topic: 'down/sensor1', op: 'publish' },
  { topic: '$SYS/broker/uptime', op: 'subscribe' },
  { topic: 'devices/dev1/reboot', op: 'publish' },
];

// file 源四种格式示例,内容与 playgroundRules 一一对应。
export const playgroundSources = {
  yaml: `rules:
  - { username: admin,   action: allow, permission: pubsub,    topic: "#" }
  - { username: "*",     action: deny,  permission: pubsub,    topic: "$SYS/#" }
  - { username: "*",     action: allow, permission: publish,   topic: "up/{user}" }
  - { username: "*",     action: allow, permission: subscribe, topic: "down/{user}" }
  - { username: "*",     action: allow, permission: subscribe, topic: "broadcast/#" }
  - { username: sensor1, action: allow, permission: pubsub,    topic: "devices/dev1/#" }

# 主体:username = 用户名,"*" = 所有用户;也支持 clientid = "..."
# 占位符:{user} = 当前 username,{clientid} = 当前 clientid
# 通配符:+ 单级,# 末级任意深度
# 语义:按顺序首条命中;无命中默认拒绝`,
  json: `{
  "rules": [
    { "username": "admin",   "action": "allow", "permission": "pubsub",    "topic": "#" },
    { "username": "*",       "action": "deny",  "permission": "pubsub",    "topic": "$SYS/#" },
    { "username": "*",       "action": "allow", "permission": "publish",   "topic": "up/{user}" },
    { "username": "*",       "action": "allow", "permission": "subscribe", "topic": "down/{user}" },
    { "username": "*",       "action": "allow", "permission": "subscribe", "topic": "broadcast/#" },
    { "username": "sensor1", "action": "allow", "permission": "pubsub",    "topic": "devices/dev1/#" }
  ]
}`,
  toml: `[[rules]]
username = "admin"     action = "allow" permission = "pubsub"    topic = "#"
[[rules]]
username = "*"         action = "deny"  permission = "pubsub"    topic = "$SYS/#"
[[rules]]
username = "*"         action = "allow" permission = "publish"   topic = "up/{user}"
[[rules]]
username = "*"         action = "allow" permission = "subscribe" topic = "down/{user}"
[[rules]]
username = "*"         action = "allow" permission = "subscribe" topic = "broadcast/#"
[[rules]]
username = "sensor1"   action = "allow" permission = "pubsub"    topic = "devices/dev1/#"`,
  users: `# 认证源(与 ACL 源相互独立,可分别选择 file/db)
users:
  admin:
    password: "$2y$10$7Kv9xF..."   # bcrypt 哈希
    superuser: true                 # superuser 跳过整条授权链
  sensor1:
    password: "$2y$10$x2Lq9p..."
    superuser: false

# 认证链语义:
#   找到且密码对 → allow,链停止
#   找到但密码错 → deny,链停止(防止弱源绕过强源)
#   没找到       → fall through 到下一个源`,
};
