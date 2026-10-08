package org.example.compute;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.util.Collector;

import java.math.BigDecimal;
import java.time.Instant;

/**
 * 风控特征计算函数。
 * 只计算 bankruptcy_protection，由 RiskControlJob 使用。
 *
 * 状态设计：
 * - bankruptcyState: 破产保护值（deposit +, withdraw -, play entry_fee - / reward +）
 */
public class RiskComputeFunction extends KeyedProcessFunction<String, JsonNode, String> {

    private static final ObjectMapper MAPPER = new ObjectMapper();

    // 破产保护状态
    private transient ValueState<Double> bankruptcyState;

    @Override
    public void open(Configuration parameters) {
        bankruptcyState = getRuntimeContext().getState(
                new ValueStateDescriptor<>("bankruptcy_protection", Double.class));
    }

    @Override
    public void processElement(JsonNode node, Context ctx, Collector<String> out) throws Exception {
        String tableName = node.get("_table_name").asText();
        long uid = extractUid(node);

        BigDecimal amount = getDecimal(node, "amount");
        BigDecimal entryFee = getDecimal(node, "entry_fee");
        BigDecimal reward = getDecimal(node, "reward");

        // 更新破产保护状态
        double bp = bankruptcyState.value() != null ? bankruptcyState.value() : 0.0;

        if ("user_deposit".equals(tableName)) {
            bp += amount.doubleValue();
        } else if ("user_withdraw".equals(tableName)) {
            bp -= amount.doubleValue();
        } else if ("user_play".equals(tableName)) {
            bp -= entryFee.doubleValue();
            bp += reward.doubleValue();
        } else {
            // user_event 等不影响风控特征，跳过
            return;
        }

        bankruptcyState.update(bp);

        // 构建 features JSON（只包含风控字段）
        ObjectNode features = MAPPER.createObjectNode();
        features.put("bankruptcy_protection", formatDecimal(bp));

        ObjectNode result = MAPPER.createObjectNode();
        result.put("uid", uid);
        result.set("features", features);
        result.put("updated_at", Instant.now().toString());

        out.collect(result.toString());
    }

    private long extractUid(JsonNode node) {
        return node.has("user_id") && !node.get("user_id").isNull()
                ? node.get("user_id").asLong()
                : node.get("uid").asLong();
    }

    private BigDecimal getDecimal(JsonNode node, String field) {
        JsonNode v = node.get(field);
        return (v != null && !v.isNull()) ? v.decimalValue() : BigDecimal.ZERO;
    }

    private String formatDecimal(double value) {
        return BigDecimal.valueOf(value).setScale(2, BigDecimal.ROUND_HALF_UP).toPlainString();
    }
}
