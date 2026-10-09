# 特征回溯快速上手指南

## 什么时候需要回溯？

| 场景 | 举例 |
|---|---|
| 上线新特征 | 新增"累计活跃天数"，老用户缺这个值 |
| 修复公式 bug | 破产保护值符号写反，历史值全错了 |
| 状态损坏 | Flink 作业异常重启，状态不一致 |

## 核心原则（必记）

```
1. TiDB 和 Flink 状态必须同时修，只改一边会被另一边覆盖
2. 三个时间点必须精确对齐：Stop 时刻 = SQL 截止点 = AT_TIMESTAMP
3. 状态表必须保存该作业的所有特征状态，不只是被修正的那个
```

## 操作步骤（5 步）

### Step 1：Stop 旧作业，记录时间

```bash
# 终端执行，记录 UTC 时间
date -u +"%Y-%m-%dT%H:%M:%SZ"
# 示例输出：2026-10-09T10:00:00Z ← 这就是 T_stop
```

然后去 MSF 控制台点 **Stop**。

### Step 2：SQL 写正确值到 TiDB

在 TiDB 上执行（不是 MySQL）：

```sql
-- ① 写特征表（以 bankruptcy_protection 为例）
INSERT INTO user_feature (uid, features, updated_at)
SELECT uid,
    JSON_OBJECT('bankruptcy_protection', bp),
    NOW()
FROM (
    SELECT uid,
        COALESCE(SUM(deposit),0) - COALESCE(SUM(withdraw),0)
        - COALESCE(SUM(entry_fee),0) + COALESCE(SUM(reward),0) AS bp
    FROM (
        SELECT uid, amount AS deposit, 0 AS withdraw, 0 AS entry_fee, 0 AS reward
        FROM user_deposit WHERE created_at <= 'T_STOP'
        UNION ALL
        SELECT uid, 0, amount, 0, 0 FROM user_withdraw WHERE created_at <= 'T_STOP'
        UNION ALL
        SELECT uid, 0, 0, entry_fee, reward FROM user_play WHERE created_at <= 'T_STOP'
    ) t GROUP BY uid
) r
ON DUPLICATE KEY UPDATE
    features = JSON_MERGE_PATCH(features, VALUES(features)),
    updated_at = VALUES(updated_at);

-- ② 写状态表（Flink 启动时从这里加载初始状态）
INSERT INTO feature_state (uid, state_key, state_value)
SELECT uid, 'bankruptcy_protection', CAST(bp AS CHAR)
FROM (
    -- 同上面的 SQL
) r
ON DUPLICATE KEY UPDATE state_value = VALUES(state_value);
```

> **注意**：`T_STOP` 替换为 Step 1 记录的时间。

### Step 3：构建新 JAR 并上传 S3

```bash
# CI/CD 自动执行（git push 触发），或手动：
mvn clean package -Paws,risk -DskipTests
aws s3 cp target/featurestore-risk-1.0-SNAPSHOT-shaded.jar s3://lib-lkx/msf/
```

### Step 4：MSF 配置并启动

MSF 控制台 → 应用 → 配置 → 运行时属性，添加：

| key | value |
|---|---|
| `source.init.position` | `AT_TIMESTAMP` |
| `source.init.position.timestamp` | `2026-10-09T10:00:00Z` |

然后点 **Start**。

### Step 5：验证

```sql
-- 抽查 3 个用户
SELECT uid,
    JSON_EXTRACT(features, '$.bankruptcy_protection') AS bp,
    updated_at
FROM user_feature
ORDER BY updated_at DESC LIMIT 3;
```

同时检查 Flink Dashboard：
- Backpressured = 0%
- 无报错

## 不同特征的回溯 SQL

### 破产保护值（RiskControlJob）

```sql
-- 特征：bankruptcy_protection
-- 公式：deposit + withdraw - entry_fee - reward（注意正负号）
-- 需要存状态表：bankruptcy_protection
```

### 推荐特征（RecommendJob）

```sql
-- 特征：total_amount, event_count（lifetime 状态）
-- 可以直接从 user_feature 表提取，不需要从 MySQL 重算
INSERT INTO feature_state (uid, state_key, state_value)
SELECT uid, 'lifetime',
    CONCAT('[', JSON_UNQUOTE(JSON_EXTRACT(features, '$.total_amount')),
           ',', JSON_EXTRACT(features, '$.event_count'), ']')
FROM user_feature
WHERE JSON_EXTRACT(features, '$.total_amount') IS NOT NULL;

-- 特征：rolling（小时桶状态）
-- 必须从 MySQL 源表按小时桶聚合重算
-- SQL 较复杂，见 SOP 文档
```

## 常见错误

| 错误 | 后果 | 正确做法 |
|---|---|---|
| 用了 LATEST 而不是 AT_TIMESTAMP | T_stop 到 Start 之间的数据丢失 | 必须用 AT_TIMESTAMP |
| SQL 截止点和 AT_TIMESTAMP 不一致 | 特征值偏大或偏小 | 三者必须精确对齐 |
| 状态表只存了被修正的特征 | 同作业其他特征被错误覆盖 | 必须存该作业所有特征的状态 |
| 先写 SQL 再 Stop 旧作业 | SQL 的正确值被旧作业覆盖 | 必须 Stop → SQL → Start |

## 检查清单

```
□ T_stop 已记录（UTC 格式，如 2026-10-09T10:00:00Z）
□ feature_state 表已创建
□ SQL 中 created_at <= 已替换为 T_stop
□ 特征表 SQL 执行成功
□ 状态表 SQL 执行成功（包含该作业所有特征的状态）
□ MSF Runtime Properties 已设置 AT_TIMESTAMP + timestamp
□ 新作业已 Start
□ Flink Dashboard 无异常
□ TiDB 抽查验证通过
```

## 快速参考

```
正常运行：不填 Runtime Properties（默认 TRIM_HORIZON）
回溯：    Runtime Properties 填 AT_TIMESTAMP + timestamp
只追新：  Runtime Properties 填 LATEST（回溯完成后切回正常运行用）
```
