package org.example.source;

import com.amazonaws.services.kinesisanalytics.runtime.KinesisAnalyticsRuntime;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.connectors.kinesis.FlinkKinesisConsumer;
import org.apache.flink.streaming.connectors.kinesis.config.ConsumerConfigConstants;
import org.example.config.AppConfig;

import java.io.IOException;
import java.time.Instant;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;

/**
 * Kinesis Source 工厂：风控 / 推荐两个作业共用同一套参数加载与源构建逻辑。
 * 初始消费位点可通过 MSF 运行时属性配置：source.init.position / source.init.position.timestamp。
 */
public final class KinesisSourceFactory {

    private KinesisSourceFactory() {}

    /**
     * 创建 Kinesis Source（EFO 消费，初始位点可配置）。
     */
    public static DataStream<String> create(StreamExecutionEnvironment env, String[] args) throws IOException {
        return env.addSource(new FlinkKinesisConsumer<>(
                AppConfig.KINESIS_STREAM_NAME, new SimpleStringSchema(), buildConfig(loadParams(args))));
    }

    /**
     * 加载参数：MSF 运行时属性（控制台 group/key/value）既不是 JVM 启动参数也不是系统属性，
     * 必须经 aws-kinesisanalytics-runtime 读取，否则 params.get 永远落到默认值。
     * 本地（非 MSF）返回空 map，自动回落到系统属性/启动参数；启动参数优先级最高。
     */
    public static ParameterTool loadParams(String[] args) throws IOException {
        ParameterTool params = ParameterTool.fromSystemProperties();
        for (Properties group : KinesisAnalyticsRuntime.getApplicationProperties().values()) {
            Map<String, String> flat = new HashMap<>();
            group.forEach((k, v) -> flat.put(String.valueOf(k), String.valueOf(v)));
            params = params.mergeWith(ParameterTool.fromMap(flat));
        }
        return params.mergeWith(ParameterTool.fromArgs(args));
    }

    private static Properties buildConfig(ParameterTool params) {
        Properties config = new Properties();
        config.setProperty("aws.region", AppConfig.AWS_REGION);

        // connector 只识别 flink.stream.initpos / flink.stream.initpos.timestamp，
        // 自定义键会被静默忽略并回落到默认 LATEST
        config.setProperty(ConsumerConfigConstants.STREAM_INITIAL_POSITION,
                params.get("source.init.position", "TRIM_HORIZON"));

        // AT_TIMESTAMP 模式需要额外指定时间戳
        String timestamp = params.get("source.init.position.timestamp", null);
        if (timestamp != null && !timestamp.trim().isEmpty()) {
            config.setProperty(ConsumerConfigConstants.STREAM_INITIAL_TIMESTAMP,
                    toEpochSeconds(timestamp));
        }

        // EFO 复用已注册消费者：需三者齐全，否则 connector 会走默认 LAZY 注册分支并要求 consumername。
        // 注册模式默认 LAZY（连接器自行 RegisterStreamConsumer，强制要求 consumername）；
        // 此处消费者已手动创建并提供 ARN，须显式设为 NONE，连接器才会改用 consumerarn.<流名> 校验。
        config.setProperty(ConsumerConfigConstants.RECORD_PUBLISHER_TYPE, "EFO");
        config.setProperty(ConsumerConfigConstants.EFO_REGISTRATION_TYPE, "NONE");
        config.setProperty(ConsumerConfigConstants.EFO_CONSUMER_ARN_PREFIX
                        + "." + AppConfig.KINESIS_STREAM_NAME,
                AppConfig.EFO_CONSUMER_ARN);

        return config;
    }

    /**
     * connector 默认时间戳模式带毫秒（yyyy-MM-dd'T'HH:mm:ss.SSSXXX），无毫秒的 ISO-8601 会解析失败，
     * 故统一转为 epoch 秒（connector 将非负小数直接当作 epoch 秒）。
     */
    private static String toEpochSeconds(String value) {
        String v = value.trim();
        try {
            Double.parseDouble(v);
            return v;
        } catch (NumberFormatException e) {
            return String.valueOf(Instant.parse(v).getEpochSecond());
        }
    }
}
