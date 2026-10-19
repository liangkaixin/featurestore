package org.example.risk;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.flink.api.common.restartstrategy.RestartStrategies;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.streaming.api.CheckpointingMode;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.CheckpointConfig;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.data.GenericRowData;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.data.StringData;
import org.example.config.AppConfig;
import org.example.compute.RiskComputeFunction;
import org.example.sink.IcebergSinkFactory;
import org.example.sink.TiDbSinkFactory;
import org.example.source.EventParser;
import org.example.source.KinesisSourceFactory;

import java.time.Duration;

/**
 * 风控特征 Flink 作业。
 * 
 * 架构：Kinesis Source → 事件解析 → KeyBy(uid) → 风控特征计算 → 双写（TiDB + Iceberg）
 * 只计算 bankruptcy_protection，与 RecommendJob 独立部署、独立回溯。
 */
public class RiskControlJob {

    private static final ObjectMapper MAPPER = new ObjectMapper();

    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = createExecutionEnvironment();

        DataStream<String> source = KinesisSourceFactory.create(env, args);
        DataStream<RowData> featureRows = buildPipeline(source);

        // 写入 TiDB（JSON_MERGE_PATCH 合并写入）
        featureRows.addSink(TiDbSinkFactory.create());
        // 写入 Iceberg（离线分析）
        IcebergSinkFactory.create(featureRows);

        env.execute(AppConfig.RISK_JOB_NAME);
    }

    private static StreamExecutionEnvironment createExecutionEnvironment() {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        env.enableCheckpointing(AppConfig.CHECKPOINT_INTERVAL_MS, CheckpointingMode.EXACTLY_ONCE);
        CheckpointConfig cp = env.getCheckpointConfig();
        cp.setMinPauseBetweenCheckpoints(AppConfig.CHECKPOINT_MIN_PAUSE_MS);
        cp.setCheckpointTimeout(AppConfig.CHECKPOINT_TIMEOUT_MS);
        cp.setMaxConcurrentCheckpoints(1);
        cp.setExternalizedCheckpointCleanup(
                CheckpointConfig.ExternalizedCheckpointCleanup.RETAIN_ON_CANCELLATION);
        env.setRestartStrategy(RestartStrategies.fixedDelayRestart(3, Duration.ofSeconds(10)));

        // 【排查用】禁用算子链，让 process / map / sink 在 UI 上独立显示背压与 busy 指标，
        // 便于定位吞吐瓶颈。定位完成后应移除本行以恢复 chaining（链化可减少序列化与网络开销）。
//        env.disableOperatorChaining();

        return env;
    }

    private static DataStream<RowData> buildPipeline(DataStream<String> source) {
        DataStream<JsonNode> parsed = source
                .flatMap(new EventParser())
                .returns(TypeInformation.of(JsonNode.class));

        DataStream<String> features = parsed
                .keyBy(node -> {
                    long uid = node.has("user_id") && !node.get("user_id").isNull()
                            ? node.get("user_id").asLong()
                            : node.get("uid").asLong();
                    return String.valueOf(uid);
                })
                .process(new RiskComputeFunction());

        return features.map(
                (String json) -> {
                    JsonNode node = MAPPER.readTree(json);
                    GenericRowData row = new GenericRowData(3);
                    row.setField(0, node.get("uid").asLong());
                    row.setField(1, StringData.fromString(node.get("features").toString()));
                    row.setField(2, StringData.fromString(node.get("updated_at").asText()));
                    return (RowData) row;
                },
                TypeInformation.of(RowData.class)
        );
    }
}
