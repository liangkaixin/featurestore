package org.example.sink;

import org.apache.flink.connector.jdbc.JdbcConnectionOptions;
import org.apache.flink.connector.jdbc.JdbcExecutionOptions;
import org.apache.flink.connector.jdbc.JdbcSink;
import org.apache.flink.streaming.api.functions.sink.SinkFunction;
import org.apache.flink.table.data.RowData;
import org.example.config.AppConfig;

/**
 * TiDB Sink 工厂：创建 JDBC Sink，使用 JSON_MERGE_PATCH 实现多作业合并写入。
 * 语义：at-least-once + ON DUPLICATE KEY UPDATE with JSON_MERGE_PATCH（幂等、不互相覆盖）
 */
public final class TiDbSinkFactory {

    private TiDbSinkFactory() {}

    /**
     * 创建 TiDB JDBC Sink，features 使用 JSON_MERGE_PATCH 合并写入。
     * 多个作业各自写入自己的特征字段，TiDB 自动合并到同一行。
     */
    public static SinkFunction<RowData> create() {
        String upsertSql = String.format(
                "INSERT INTO %s (uid, features, updated_at) VALUES (?, ?, ?) " +
                "ON DUPLICATE KEY UPDATE " +
                "features = JSON_MERGE_PATCH(features, VALUES(features)), " +
                "updated_at = VALUES(updated_at)",
                AppConfig.TIDB_TABLE
        );

        return JdbcSink.sink(
                upsertSql,
                (ps, row) -> {
                    ps.setLong(1, row.getLong(0));
                    ps.setString(2, row.getString(1).toString());
                    ps.setString(3, row.getString(2).toString());
                },
                JdbcExecutionOptions.builder()
                        .withBatchSize(500)         // 攒 500 条批量写入，减少网络往返
                        .withBatchIntervalMs(1000)  // 最多等 1 秒，平衡延迟和吞吐
                        .withMaxRetries(3)
                        .build(),
                new JdbcConnectionOptions.JdbcConnectionOptionsBuilder()
                        .withUrl(AppConfig.TIDB_JDBC_URL)
                        .withUsername(AppConfig.TIDB_USERNAME)
                        .withPassword(AppConfig.TIDB_PASSWORD)
                        .build()
        );
    }
}
