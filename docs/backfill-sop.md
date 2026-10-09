# 特征回溯标准操作流程（SOP）

## 一、适用场景

| 场景 | 说明 | 示例 |
|---|---|---|
| 新增特征 | 上线新的特征字段，老用户缺少该特征值 | 新增"累计活跃天数" |
| 修正公式 | 特征计算公式有 bug，TiDB 和 Flink 状态都是错的 | 破产保护值公式符号写反 |
| 状态损坏 | Flink Checkpoint 损坏，状态丢失或不一致 | 作业异常重启后状态异常 |

## 二、核心原则

```
1. TiDB 和 Flink 状态必须同时修正，否则一边修了也会被另一边覆盖
2. 三个时间点必须对齐：Stop 旧作业时刻 = SQL 计算截止点 = AT_TIMESTAMP 时间戳
3. 回溯期间 Kinesis 事件不丢（AT_TIMESTAMP 会消费 Stop 之后的积压数据）
4. 状态表必须保存该作业的所有特征状态，不只是被修正的那个
```

## 二-B、状态保存规则（重要）

回溯某个特征时，**必须同时保存同作业所有特征的状态**，否则其他特征会被错误覆盖。

| 作业 | 特征 | 状态键 | 能否从特征表提取 |
|---|---|---|---|
| RiskControlJob | bankruptcy_protection | `bankruptcy_protection` | ✅ 可以 |
| RecommendJob | total_amount, event_count | `lifetime` | ✅ 可以（从 user_feature 表提取） |
| RecommendJob | rolling 窗口特征 | `rolling` | ❌ 必须从 MySQL 源表重算 |

```sql
-- lifetime 可以直接从特征表提取，不需要重算：
INSERT INTO feature_state (uid, state_key, state_value)
SELECT uid, 'lifetime',
    CONCAT('[', JSON_UNQUOTE(JSON_EXTRACT(features, '$.total_amount')),
           ',', JSON_EXTRACT(features, '$.event_count'), ']')
FROM user_feature
WHERE JSON_EXTRACT(features, '$.total_amount') IS NOT NULL;
```

## 三、前置准备

### 3.1 确认 TiDB 状态表已创建

```sql
CREATE TABLE IF NOT EXISTS feature_state (
    uid         BIGINT NOT NULL,
    state_key   VARCHAR(64) NOT NULL,
    state_value TEXT NOT NULL,
    updated_at  DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
    PRIMARY KEY (uid, state_key)
);
```

### 3.2 确认 Kinesis 保留期

```bash
# 查看当前保留期
aws kinesis describe-stream --stream-name test --region us-east-1 \
  --query 'StreamDescription.RetentionPeriodHours'

# 建议设为最大值 7 天
aws kinesis increase-stream-retention-period \
  --stream-name test --region us-east-1 \
  --retention-period-hours 168
```

### 3.3 确认代码已包含状态表加载逻辑

当前代码已实现：
- `RiskComputeFunction` — 首次处理 uid 时从 `feature_state` 读取 `bankruptcy_protection`
- `RecommendComputeFunction` — 首次处理 uid 时从 `feature_state` 读取 `lifetime` 和 `rolling`

## 四、操作流程

### 场景 A：新增特征（无序列依赖）

适用：累计金额、活跃天数等加法可交换的特征。

```
步骤 1: 开发新特征代码
步骤 2: 构建 JAR → 上传 S3
步骤 3: Stop 旧作业 → 记录 T_stop
步骤 4: SQL 写特征表 + 状态表
步骤 5: Start 新作业（AT_TIMESTAMP = T_stop）
步骤 6: 验证
```

#### 步骤 3：Stop 旧作业，记录时间戳

```bash
# 记录 Stop 时间（UTC）
T_STOP=$(date -u +"%Y-%m-%dT%H:%M:%SZ")
echo "T_stop = $T_STOP"
# 输出示例：T_stop = 2026-10-09T10:00:00Z
```

在 MSF 控制台点击 **Stop**。

#### 步骤 4：SQL 写特征表 + 状态表

**写特征表**（以新增 `active_days` 为例）：

```sql
INSERT INTO user_feature (uid, features, updated_at)
SELECT uid, JSON_OBJECT('active_days', active_days), NOW()
FROM (
    SELECT uid, COUNT(DISTINCT DATE(created_at)) AS active_days
    FROM user_event
    WHERE created_at <= 'T_STOP 替换为实际时间'
    GROUP BY uid
) t
ON DUPLICATE KEY UPDATE
    features = JSON_MERGE_PATCH(features, VALUES(features)),
    updated_at = VALUES(updated_at);
```

**写状态表**（如果新特征有 Flink 状态）：

```sql
INSERT INTO feature_state (uid, state_key, state_value)
SELECT uid, 'active_days', CAST(active_days AS CHAR)
FROM (
    SELECT uid, COUNT(DISTINCT DATE(created_at)) AS active_days
    FROM user_event
    WHERE created_at <= 'T_STOP 替换为实际时间'
    GROUP BY uid
) t
ON DUPLICATE KEY UPDATE state_value = VALUES(state_value);
```

#### 步骤 5：Start 新作业

MSF 控制台：
- 更新 S3 代码路径为新 JAR
- Runtime Properties 设置：
  ```
  source.init.position = AT_TIMESTAMP
  source.init.position.timestamp = T_STOP 替换为实际时间
  ```
- 点击 **Start**

---

### 场景 B：修正公式（有状态依赖）

适用：破产保护值公式修改、连续登录天数等有状态依赖的特征。

```
步骤 1: 修复代码（正确的公式）
步骤 2: 构建 JAR → 上传 S3
步骤 3: Stop 旧作业 → 记录 T_stop
步骤 4: SQL 从 MySQL 源表重算 → 写特征表 + 状态表
步骤 5: Start 新作业（AT_TIMESTAMP = T_stop）
步骤 6: 验证
```

#### 步骤 4：从 MySQL 源表重算（以破产保护值为例）

**写特征表**：

```sql
INSERT INTO user_feature (uid, features, updated_at)
SELECT uid, JSON_OBJECT('bankruptcy_protection', bp), NOW()
FROM (
    SELECT uid,
        COALESCE(SUM(deposit), 0)
        - COALESCE(SUM(withdraw), 0)
        - COALESCE(SUM(entry_fee), 0)
        + COALESCE(SUM(reward), 0) AS bp
    FROM (
        SELECT uid, amount AS deposit, 0 AS withdraw, 0 AS entry_fee, 0 AS reward
        FROM user_deposit WHERE created_at <= 'T_STOP'
        UNION ALL
        SELECT uid, 0, amount, 0, 0
        FROM user_withdraw WHERE created_at <= 'T_STOP'
        UNION ALL
        SELECT uid, 0, 0, entry_fee, reward
        FROM user_play WHERE created_at <= 'T_STOP'
    ) t GROUP BY uid
) r
ON DUPLICATE KEY UPDATE
    features = JSON_MERGE_PATCH(features, VALUES(features)),
    updated_at = VALUES(updated_at);
```

**写状态表**：

```sql
INSERT INTO feature_state (uid, state_key, state_value)
SELECT uid, 'bankruptcy_protection', CAST(bp AS CHAR)
FROM (
    SELECT uid,
        COALESCE(SUM(deposit), 0)
        - COALESCE(SUM(withdraw), 0)
        - COALESCE(SUM(entry_fee), 0)
        + COALESCE(SUM(reward), 0) AS bp
    FROM (
        SELECT uid, amount AS deposit, 0 AS withdraw, 0 AS entry_fee, 0 AS reward
        FROM user_deposit WHERE created_at <= 'T_STOP'
        UNION ALL
        SELECT uid, 0, amount, 0, 0
        FROM user_withdraw WHERE created_at <= 'T_STOP'
        UNION ALL
        SELECT uid, 0, 0, entry_fee, reward
        FROM user_play WHERE created_at <= 'T_STOP'
    ) t GROUP BY uid
) r
ON DUPLICATE KEY UPDATE state_value = VALUES(state_value);
```

**RecommendComputeFunction 的状态表写入**（lifetime + rolling）：

```sql
-- lifetime 状态
INSERT INTO feature_state (uid, state_key, state_value)
SELECT uid, 'lifetime', CONCAT('[', total_amount, ',', event_count, ']')
FROM (
    SELECT uid,
        COALESCE(SUM(amount), 0) AS total_amount,
        COUNT(*) AS event_count
    FROM user_event WHERE created_at <= 'T_STOP'
    GROUP BY uid
) t
ON DUPLICATE KEY UPDATE state_value = VALUES(state_value);

-- rolling 状态（需要按小时桶聚合）
-- 建议通过 BackfillJob 计算后写入，而非手写 SQL
```

---

### 场景 C：状态损坏（Checkpoint 异常）

```
步骤 1: Stop 异常作业
步骤 2: 确认代码无 bug（如果只是状态损坏，不需要改代码）
步骤 3: SQL 从 MySQL 源表重算 → 写特征表 + 状态表
步骤 4: Start 新作业（AT_TIMESTAMP = T_stop）
步骤 5: 验证
```

与场景 B 步骤相同，只是不需要修改代码。

## 五、验证清单

### 5.1 抽查特征值

```sql
-- 查 TiDB 特征表
SELECT uid,
       JSON_EXTRACT(features, '$.bankruptcy_protection') AS bp,
       updated_at
FROM user_feature
ORDER BY updated_at DESC
LIMIT 10;
```

### 5.2 交叉验证

```sql
-- 选一个 uid，手动从 MySQL 源表计算
SELECT
    (SELECT COALESCE(SUM(amount), 0) FROM user_deposit WHERE uid = 1001) AS deposit,
    (SELECT COALESCE(SUM(amount), 0) FROM user_withdraw WHERE uid = 1001) AS withdraw,
    (SELECT COALESCE(SUM(entry_fee), 0) FROM user_play WHERE uid = 1001) AS entry_fee,
    (SELECT COALESCE(SUM(reward), 0) FROM user_play WHERE uid = 1001) AS reward;

-- 手动计算: deposit - withdraw - entry_fee + reward
-- 和 TiDB 里的 bankruptcy_protection 对比，应一致
```

### 5.3 确认实时写入正常

```sql
-- 等几分钟，确认有新数据更新
SELECT uid, features, updated_at
FROM user_feature
ORDER BY updated_at DESC
LIMIT 5;
```

### 5.4 确认 Flink Dashboard 正常

- Backpressured = 0%
- Busy 在合理范围
- 无报错日志

## 六、回滚方案

如果回溯后发现问题：

```
1. Stop 新作业
2. 重新执行步骤 4（SQL 重算，用正确的逻辑）
3. Start 新作业（AT_TIMESTAMP = 新的 T_stop）
```

状态表支持 `ON DUPLICATE KEY UPDATE`，重复执行不会出错。

## 七、时间估算

| 用户量 | SQL 重算 | 部署 | 总计停机 |
|---|---|---|---|
| 9,000 | ~10 秒 | ~30 秒 | **~1 分钟** |
| 100 万 | ~1 分钟 | ~30 秒 | **~2 分钟** |
| 1000 万 | ~10 分钟 | ~30 秒 | **~11 分钟** |

## 八、关键注意事项

1. **T_stop 必须精确对齐**：SQL 的 `WHERE created_at <= 'T_STOP'` 和 `source.init.position.timestamp` 必须是同一个时间点
2. **先 Stop 再 SQL**：否则旧作业的写入会覆盖 SQL 的正确值
3. **SQL 必须在 Start 之前完成**：否则新作业读到的状态表是旧值
4. **AT_TIMESTAMP 不是 LATEST**：LATEST 会丢失 T_stop 到 Start 之间的数据
5. **状态表可重复写入**：`ON DUPLICATE KEY UPDATE` 保证幂等性
6. **同作业所有特征状态必须一起保存**：否则未保存的特征会被 Flink 从零开始计算，覆盖正确值
7. **RiskControlJob 回溯不影响 RecommendJob**：两个作业独立部署，只停被修正的那个

## 九、操作检查表

```
□ 代码已修改并测试
□ JAR 已构建并上传 S3
□ feature_state 表已创建
□ Kinesis 保留期 ≥ 7 天
□ T_stop 已记录（UTC 格式）
□ SQL 中的 created_at <= T_stop 已替换
□ 特征表 SQL 已执行成功
□ 状态表 SQL 已执行成功（包含该作业所有特征的状态）
□ 同作业其他特征的状态也已保存到状态表
□ 新作业 AT_TIMESTAMP = T_stop 已配置
□ 新作业已 Start
□ Flink Dashboard 无异常
□ TiDB 抽查验证通过
□ MySQL 交叉验证通过
```
