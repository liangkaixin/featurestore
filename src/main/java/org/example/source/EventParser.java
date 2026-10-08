package org.example.source;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.util.Collector;

/**
 * 事件解析器：解析 JSON 消息，过滤有效事件（insert/update），附加表名到数据节点。
 * 输入格式：{"data": {...}, "metadata": {"table-name": "xxx", "operation": "insert"}}
 * 输出格式：data 节点 + _table_name 字段
 */
public class EventParser implements FlatMapFunction<String, JsonNode> {

    private static final ObjectMapper MAPPER = new ObjectMapper();

    @Override
    public void flatMap(String value, Collector<JsonNode> out) throws Exception {
        JsonNode root = MAPPER.readTree(value);
        JsonNode data = root.get("data");
        JsonNode metadata = root.get("metadata");

        if (data == null || metadata == null) return;

        String tableName = metadata.has("table-name") ? metadata.get("table-name").asText() : null;
        String operation = metadata.has("operation") ? metadata.get("operation").asText() : null;

        if (tableName == null || operation == null) return;
        if (!"insert".equals(operation) && !"update".equals(operation)) return;

        // 附加 metadata 到 data 节点，方便后续处理
        ObjectNode merged = data.deepCopy();
        merged.put("_table_name", tableName);
        out.collect(merged);
    }
}
