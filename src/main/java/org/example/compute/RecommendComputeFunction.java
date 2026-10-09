package org.example.compute;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;
import org.apache.flink.api.common.state.MapState;
import org.apache.flink.api.common.state.MapStateDescriptor;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.util.Collector;
import org.example.config.AppConfig;

import java.math.BigDecimal;
import java.time.Instant;
import java.util.Map;

/**
 * 推荐特征计算函数。
 * 计算除 bankruptcy_protection 之外的所有特征，由 RecommendJob 使用。
 *
 * 状态设计：
 * - lifetimeState: 全量历史聚合 [total_amount, event_count]
 * - rollingState: 按小时桶聚合 (hourEpoch → [game_count, gtv_sum])
 * - initialized: 是否已从 feature_state 表加载过初始状态（回溯/修正场景）
 */
public class RecommendComputeFunction extends KeyedProcessFunction<String, JsonNode, String> {

    private static final ObjectMapper MAPPER = new ObjectMapper();
    private static final String STATE_KEY_LIFETIME = "lifetime";
    private static final String STATE_KEY_ROLLING = "rolling";

    // Lifetime 状态：[total_amount, event_count]
    private transient ValueState<double[]> lifetimeState;

    // Rolling 状态：按小时桶聚合 (hourEpoch → [game_count, gtv_sum])
    private transient MapState<Long, double[]> rollingState;

    // 是否已从状态表加载（回溯/修正时用于衔接历史状态）
    private transient ValueState<Boolean> initialized;

    // 状态表加载器（HikariCP 连接池）
    private transient StateTableLoader stateTableLoader;

    @Override
    public void open(Configuration parameters) {
        lifetimeState = getRuntimeContext().getState(
                new ValueStateDescriptor<>("lifetime", double[].class));

        rollingState = getRuntimeContext().getMapState(
                new MapStateDescriptor<>("rolling",
                        TypeInformation.of(Long.class),
                        TypeInformation.of(double[].class)));

        initialized = getRuntimeContext().getState(
                new ValueStateDescriptor<>("initialized", Boolean.class));

        stateTableLoader = new StateTableLoader();
        stateTableLoader.open();
    }

    @Override
    public void close() {
        if (stateTableLoader != null) {
            stateTableLoader.close();
        }
    }

    @Override
    public void processElement(JsonNode node, Context ctx, Collector<String> out) throws Exception {
        String tableName = node.get("_table_name").asText();
        long uid = extractUid(node);

        // 首次处理该 uid → 从 feature_state 表加载初始状态（回溯/修正场景）
        if (initialized.value() == null) {
            loadStateFromTable(uid);
            initialized.update(true);
        }

        BigDecimal amount = getDecimal(node, "amount");
        BigDecimal entryFee = getDecimal(node, "entry_fee");
        long eventTimeMs = getNodeTimeMs(node);

        // 1. 更新 Lifetime 状态（只处理 user_event）
        if ("user_event".equals(tableName)) {
            double[] lt = lifetimeState.value();
            if (lt == null) lt = new double[2];
            lt[0] += amount.doubleValue();   // total_amount
            lt[1] += 1;                       // event_count
            lifetimeState.update(lt);
        }

        // 2. 更新 Rolling 小时桶（只处理 user_play）
        if ("user_play".equals(tableName)) {
            long hourKey = eventTimeMs / 3_600_000;
            double[] bucket = rollingState.get(hourKey);
            if (bucket == null) bucket = new double[2];
            bucket[0] += 1;                          // game_count
            bucket[1] += entryFee.doubleValue();      // gtv_sum
            rollingState.put(hourKey, bucket);

            // 清理过期桶
            cleanupExpiredBuckets(hourKey);
        }

        // 3. 构建 features JSON（只包含推荐字段）
        String result = buildResultJson(uid, entryFee);
        out.collect(result);
    }

    private long extractUid(JsonNode node) {
        return node.has("user_id") && !node.get("user_id").isNull()
                ? node.get("user_id").asLong()
                : node.get("uid").asLong();
    }

    private void cleanupExpiredBuckets(long currentHourKey) throws Exception {
        long cutoffHour = currentHourKey - AppConfig.ROLLING_WINDOW_HOURS;
        java.util.Iterator<Long> keyIter = rollingState.keys().iterator();
        while (keyIter.hasNext()) {
            if (keyIter.next() < cutoffHour) {
                keyIter.remove();
            }
        }
    }

    /**
     * 从 feature_state 表加载 lifetime 和 rolling 状态。
     * 回溯/修正场景：SQL 先算出正确值写入状态表，Flink 启动时从状态表加载，实现状态衔接。
     */
    private void loadStateFromTable(long uid) throws Exception {
        // 加载 lifetime 状态
        double[] savedLifetime = stateTableLoader.loadDoubleArray(uid, STATE_KEY_LIFETIME);
        if (savedLifetime != null) {
            lifetimeState.update(savedLifetime);
        }

        // 加载 rolling 状态（putAll 批量写入，避免逐条 put 的序列化开销）
        Map<Long, double[]> savedRolling = stateTableLoader.loadMap(uid, STATE_KEY_ROLLING);
        if (savedRolling != null) {
            rollingState.putAll(savedRolling);
        }
    }

    private String buildResultJson(long uid, BigDecimal entryFee) throws Exception {
        double[] lt = lifetimeState.value();
        if (lt == null) lt = new double[2];

        // 计算 Rolling 特征
        long currentHour = System.currentTimeMillis() / 3_600_000;
        long h72 = currentHour - AppConfig.RECENT_HOURS_72H;
        double gameCount72h = 0, gtv72h = 0;
        double gameCount216h = 0, gtv216h = 0;

        for (java.util.Map.Entry<Long, double[]> entry : rollingState.entries()) {
            long h = entry.getKey();
            double[] b = entry.getValue();
            if (h > h72) {
                gameCount72h += b[0];
                gtv72h += b[1];
            }
            gameCount216h += b[0];
            gtv216h += b[1];
        }

        double gameCount12to4d = gameCount216h - gameCount72h;
        double gtv12to4d = gtv216h - gtv72h;

        ObjectNode features = MAPPER.createObjectNode();
        // Lifetime 特征
        features.put("total_amount", formatDecimal(lt[0]));
        features.put("event_count", (int) lt[1]);
        // Rolling 特征
        features.put("prev_3days_game_count", (int) gameCount72h);
        features.put("prev_12to4days_game_count", (int) gameCount12to4d);
        features.put("prev_3days_gtv", formatDecimal(gtv72h));
        features.put("prev_12to4days_gtv", formatDecimal(gtv12to4d));
        features.put("prev_3days_avg_entry_fee",
                formatDecimal(gameCount72h > 0 ? gtv72h / gameCount72h : 0));
        features.put("prev_12to4days_avg_entry_fee",
                formatDecimal(gameCount12to4d > 0 ? gtv12to4d / gameCount12to4d : 0));
        features.put("real_entry_fee_cash_bonus", formatDecimal(entryFee.doubleValue()));

        ObjectNode result = MAPPER.createObjectNode();
        result.put("uid", uid);
        result.set("features", features);
        result.put("updated_at", Instant.now().toString());

        return result.toString();
    }

    private BigDecimal getDecimal(JsonNode node, String field) {
        JsonNode v = node.get(field);
        return (v != null && !v.isNull()) ? v.decimalValue() : BigDecimal.ZERO;
    }

    private long getNodeTimeMs(JsonNode node) {
        for (String field : new String[]{"event_time", "created_at"}) {
            JsonNode v = node.get(field);
            if (v != null && !v.isNull()) {
                try { return Instant.parse(v.asText()).toEpochMilli(); } catch (Exception ignored) {}
            }
        }
        return System.currentTimeMillis();
    }

    private String formatDecimal(double value) {
        return BigDecimal.valueOf(value).setScale(2, BigDecimal.ROUND_HALF_UP).toPlainString();
    }
}
