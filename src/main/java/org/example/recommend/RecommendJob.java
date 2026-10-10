package org.example.recommend;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.flink.api.common.restartstrategy.RestartStrategies;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.streaming.api.CheckpointingMode;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.CheckpointConfig;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.connectors.kinesis.FlinkKinesisConsumer;
import org.apache.flink.table.data.GenericRowData;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.data.StringData;
import org.example.config.AppConfig;
import org.example.compute.RecommendComputeFunction;
import org.example.sink.IcebergSinkFactory;
import org.example.sink.TiDbSinkFactory;
import org.example.source.EventParser;

import java.time.Duration;
import java.util.Properties;

/**
 * 推荐特征 Flink 作业。
 * 
 * 架构：Kinesis Source → 事件解析 → KeyBy(uid) → 推荐特征计算 → 双写（TiDB + Iceberg）
 * 计算除 bankruptcy_protection 之外的所有特征，与 RiskControlJob 独立部署、独立回溯。
 */
public class RecommendJob {

    private static final ObjectMapper MAPPER = new ObjectMapper();

    public static void main(String[] args) throws Exception {
        ParameterTool params = ParameterTool.fromArgs(args);
        StreamExecutionEnvironment env = createExecutionEnvironment(params);

        DataStream<String> source = createKinesisSource(env, params);
        DataStream<RowData> featureRows = buildPipeline(source);

        // 写入 TiDB（JSON_MERGE_PATCH 合并写入）
        featureRows.addSink(TiDbSinkFactory.create());
        // 写入 Iceberg（离线分析）
        IcebergSinkFactory.create(featureRows);

        env.execute(AppConfig.RECOMMEND_JOB_NAME);
    }

    private static StreamExecutionEnvironment createExecutionEnvironment(ParameterTool params) {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        env.enableCheckpointing(AppConfig.CHECKPOINT_INTERVAL_MS, CheckpointingMode.EXACTLY_ONCE);
        CheckpointConfig cp = env.getCheckpointConfig();
        cp.setMinPauseBetweenCheckpoints(AppConfig.CHECKPOINT_MIN_PAUSE_MS);
        cp.setCheckpointTimeout(AppConfig.CHECKPOINT_TIMEOUT_MS);
        cp.setMaxConcurrentCheckpoints(1);
        cp.setExternalizedCheckpointCleanup(
                CheckpointConfig.ExternalizedCheckpointCleanup.RETAIN_ON_CANCELLATION);
        env.setRestartStrategy(RestartStrategies.fixedDelayRestart(3, Duration.ofSeconds(10)));

        return env;
    }

    private static DataStream<String> createKinesisSource(
            StreamExecutionEnvironment env, ParameterTool params) {

        Properties config = new Properties();
        config.setProperty("aws.region", AppConfig.AWS_REGION);
        config.setProperty("source.init.position",
                params.get("source.init.position", "TRIM_HORIZON"));

        // AT_TIMESTAMP 模式需要额外指定时间戳
        String timestamp = params.get("source.init.position.timestamp", null);
        if (timestamp != null) {
            config.setProperty("source.init.position.timestamp", timestamp);
        }

        config.setProperty("stream.extra.consumer.arn", AppConfig.EFO_CONSUMER_ARN);

        return env.addSource(
                new FlinkKinesisConsumer<>(AppConfig.KINESIS_STREAM_NAME,
                        new SimpleStringSchema(), config));
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
                .process(new RecommendComputeFunction());

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
