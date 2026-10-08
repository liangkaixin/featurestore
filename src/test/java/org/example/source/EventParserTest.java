package org.example.source;

import com.fasterxml.jackson.databind.JsonNode;
import org.apache.flink.util.Collector;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.List;

import static org.junit.jupiter.api.Assertions.*;

/**
 * EventParser 单元测试。
 */
class EventParserTest {

    private EventParser parser;
    private TestCollector<JsonNode> out;

    @BeforeEach
    void setUp() {
        parser = new EventParser();
        out = new TestCollector<>();
    }

    @Test
    void testValidInsertEvent() throws Exception {
        String json = "{\"data\":{\"uid\":1001,\"amount\":99.9}," +
                "\"metadata\":{\"operation\":\"insert\",\"table-name\":\"user_event\"}}";

        parser.flatMap(json, out);

        assertEquals(1, out.collected.size());
        assertEquals("user_event", out.collected.get(0).get("_table_name").asText());
        assertEquals(1001, out.collected.get(0).get("uid").asLong());
    }

    @Test
    void testValidUpdateEvent() throws Exception {
        String json = "{\"data\":{\"uid\":1002,\"amount\":50.0}," +
                "\"metadata\":{\"operation\":\"update\",\"table-name\":\"user_deposit\"}}";

        parser.flatMap(json, out);

        assertEquals(1, out.collected.size());
        assertEquals("user_deposit", out.collected.get(0).get("_table_name").asText());
    }

    @Test
    void testDeleteEventFiltered() throws Exception {
        String json = "{\"data\":{\"uid\":1003}," +
                "\"metadata\":{\"operation\":\"delete\",\"table-name\":\"user_event\"}}";

        parser.flatMap(json, out);

        assertEquals(0, out.collected.size());
    }

    @Test
    void testMissingDataField() throws Exception {
        String json = "{\"metadata\":{\"operation\":\"insert\",\"table-name\":\"user_event\"}}";

        parser.flatMap(json, out);

        assertEquals(0, out.collected.size());
    }

    @Test
    void testMissingMetadataField() throws Exception {
        String json = "{\"data\":{\"uid\":1001}}";

        parser.flatMap(json, out);

        assertEquals(0, out.collected.size());
    }

    @Test
    void testMissingTableName() throws Exception {
        String json = "{\"data\":{\"uid\":1001}," +
                "\"metadata\":{\"operation\":\"insert\"}}";

        parser.flatMap(json, out);

        assertEquals(0, out.collected.size());
    }

    @Test
    void testMissingOperation() throws Exception {
        String json = "{\"data\":{\"uid\":1001}," +
                "\"metadata\":{\"table-name\":\"user_event\"}}";

        parser.flatMap(json, out);

        assertEquals(0, out.collected.size());
    }

    @Test
    void testInvalidJsonSilentlyFails() throws Exception {
        // 当前实现中，无效 JSON 会抛异常（由 Flink 框架处理）
        // 这里验证 flatMap 对无效输入的行为
        assertThrows(Exception.class, () -> parser.flatMap("not a json", out));
    }

    @Test
    void testAllTableTypes() throws Exception {
        String[] tables = {"user_event", "user_deposit", "user_withdraw", "user_play"};
        for (String table : tables) {
            setUp(); // reset
            String json = String.format(
                    "{\"data\":{\"uid\":1001},\"metadata\":{\"operation\":\"insert\",\"table-name\":\"%s\"}}", table);

            parser.flatMap(json, out);

            assertEquals(1, out.collected.size(), "Table " + table + " should pass");
            assertEquals(table, out.collected.get(0).get("_table_name").asText());
        }
    }

    @Test
    void testDataFieldsPreserved() throws Exception {
        String json = "{\"data\":{\"uid\":1001,\"amount\":99.9,\"event_type\":\"purchase\"}," +
                "\"metadata\":{\"operation\":\"insert\",\"table-name\":\"user_event\"}}";

        parser.flatMap(json, out);

        assertEquals(1, out.collected.size());
        JsonNode result = out.collected.get(0);
        assertEquals(99.9, result.get("amount").asDouble());
        assertEquals("purchase", result.get("event_type").asText());
        assertEquals("user_event", result.get("_table_name").asText());
    }

    @Test
    void testMetadataNotLeakedToOutput() throws Exception {
        String json = "{\"data\":{\"uid\":1001}," +
                "\"metadata\":{\"operation\":\"insert\",\"table-name\":\"user_event\"}}";

        parser.flatMap(json, out);

        assertEquals(1, out.collected.size());
        JsonNode result = out.collected.get(0);
        // metadata 本身不应出现在输出中，只有 _table_name 被附加
        assertNull(result.get("metadata"));
        assertNull(result.get("operation"));
    }

    // --- 测试辅助类 ---

    private static class TestCollector<T> implements Collector<T> {
        List<T> collected = new ArrayList<>();

        @Override
        public void collect(T record) { collected.add(record); }

        @Override
        public void close() {}
    }
}
