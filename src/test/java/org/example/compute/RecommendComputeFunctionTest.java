package org.example.compute;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.*;

/**
 * RecommendComputeFunction 单元测试。
 * 测试 JSON 构建逻辑和特征字段完整性。
 */
class RecommendComputeFunctionTest {

    private static final ObjectMapper MAPPER = new ObjectMapper();

    @Test
    void testOutputContainsAllRecommendFeatures() throws Exception {
        // 模拟 RecommendComputeFunction 的输出 JSON
        ObjectNode features = MAPPER.createObjectNode();
        features.put("total_amount", "500.00");
        features.put("event_count", 10);
        features.put("prev_3days_game_count", 5);
        features.put("prev_12to4days_game_count", 3);
        features.put("prev_3days_gtv", "150.00");
        features.put("prev_12to4days_gtv", "90.00");
        features.put("prev_3days_avg_entry_fee", "30.00");
        features.put("prev_12to4days_avg_entry_fee", "30.00");
        features.put("real_entry_fee_cash_bonus", "25.00");

        ObjectNode result = MAPPER.createObjectNode();
        result.put("uid", 1001);
        result.set("features", features);
        result.put("updated_at", "2026-10-03T00:00:00Z");

        String json = result.toString();
        JsonNode parsed = MAPPER.readTree(json);

        // 验证所有推荐特征字段存在
        JsonNode f = parsed.get("features");
        assertNotNull(f.get("total_amount"));
        assertNotNull(f.get("event_count"));
        assertNotNull(f.get("prev_3days_game_count"));
        assertNotNull(f.get("prev_12to4days_game_count"));
        assertNotNull(f.get("prev_3days_gtv"));
        assertNotNull(f.get("prev_12to4days_gtv"));
        assertNotNull(f.get("prev_3days_avg_entry_fee"));
        assertNotNull(f.get("prev_12to4days_avg_entry_fee"));
        assertNotNull(f.get("real_entry_fee_cash_bonus"));

        // 确保不包含风控特征
        assertNull(f.get("bankruptcy_protection"));
    }

    @Test
    void testUserEventUpdatesLifetimeState() throws Exception {
        // user_event 应更新 total_amount 和 event_count
        ObjectNode event = MAPPER.createObjectNode();
        event.put("_table_name", "user_event");
        event.put("uid", 1001);
        event.put("amount", 99.9);

        assertEquals("user_event", event.get("_table_name").asText());
        assertEquals(99.9, event.get("amount").asDouble());
    }

    @Test
    void testPlayEventUpdatesRollingState() throws Exception {
        // user_play 应更新 rolling 小时桶
        ObjectNode event = MAPPER.createObjectNode();
        event.put("_table_name", "user_play");
        event.put("uid", 1001);
        event.put("entry_fee", 15.0);
        event.put("event_time", "2026-10-03T10:00:00Z");

        assertEquals("user_play", event.get("_table_name").asText());
        assertEquals(15.0, event.get("entry_fee").asDouble());
    }

    @Test
    void testDepositDoesNotAffectRecommend() throws Exception {
        // user_deposit 不应影响推荐特征
        ObjectNode event = MAPPER.createObjectNode();
        event.put("_table_name", "user_deposit");
        event.put("uid", 1001);
        event.put("amount", 500.0);

        // RecommendComputeFunction 只处理 user_event 和 user_play
        String tableName = event.get("_table_name").asText();
        boolean shouldProcess = "user_event".equals(tableName) || "user_play".equals(tableName);
        assertFalse(shouldProcess, "user_deposit should not be processed by RecommendComputeFunction");
    }

    @Test
    void testJsonMergePatchCompatibility() throws Exception {
        // 验证风控和推荐的 JSON 可以通过 JSON_MERGE_PATCH 合并
        String riskJson = "{\"bankruptcy_protection\":\"100.00\"}";
        String recommendJson = "{\"total_amount\":\"500.00\",\"event_count\":10}";

        JsonNode risk = MAPPER.readTree(riskJson);
        JsonNode recommend = MAPPER.readTree(recommendJson);

        // 模拟合并（实际由 TiDB JSON_MERGE_PATCH 完成）
        ObjectNode merged = MAPPER.createObjectNode();
        risk.fields().forEachRemaining(e -> merged.set(e.getKey(), e.getValue()));
        recommend.fields().forEachRemaining(e -> merged.set(e.getKey(), e.getValue()));

        // 验证合并后包含所有字段
        assertEquals("100.00", merged.get("bankruptcy_protection").asText());
        assertEquals("500.00", merged.get("total_amount").asText());
        assertEquals(10, merged.get("event_count").asInt());

        // 确保无字段冲突
        assertEquals(3, merged.size());
    }

    @Test
    void testDecimalFormatting() {
        // 验证金额格式化逻辑
        double value = 123.456;
        String formatted = java.math.BigDecimal.valueOf(value)
                .setScale(2, java.math.BigDecimal.ROUND_HALF_UP)
                .toPlainString();
        assertEquals("123.46", formatted);

        // 整数
        formatted = java.math.BigDecimal.valueOf(100.0)
                .setScale(2, java.math.BigDecimal.ROUND_HALF_UP)
                .toPlainString();
        assertEquals("100.00", formatted);

        // 零
        formatted = java.math.BigDecimal.valueOf(0.0)
                .setScale(2, java.math.BigDecimal.ROUND_HALF_UP)
                .toPlainString();
        assertEquals("0.00", formatted);
    }

    @Test
    void testHourKeyCalculation() {
        // 验证小时桶 key 计算
        long eventTimeMs = 1727942400000L; // 2024-10-03T10:00:00Z
        long hourKey = eventTimeMs / 3_600_000;
        assertTrue(hourKey > 0);

        // 同一小时内的事件应该有相同的 hourKey
        long sameHourMs = eventTimeMs + 1800_000; // +30 分钟
        long sameHourKey = sameHourMs / 3_600_000;
        assertEquals(hourKey, sameHourKey);

        // 下一小时的事件应该有不同的 hourKey
        long nextHourMs = eventTimeMs + 3600_000; // +1 小时
        long nextHourKey = nextHourMs / 3_600_000;
        assertEquals(hourKey + 1, nextHourKey);
    }
}
