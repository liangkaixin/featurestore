package org.example.config;

/**
 * 应用配置常量。
 * 生产环境建议从环境变量或配置中心读取。
 */
public final class AppConfig {

    private AppConfig() {}

    // ==================== AWS ====================
    public static final String AWS_REGION = "us-east-1";
    public static final String KINESIS_STREAM_NAME = "test";

    // ==================== Iceberg ====================
    public static final String ICEBERG_CATALOG_NAME = "glue";
    public static final String ICEBERG_WAREHOUSE = "s3://feature-store-warehouse/iceberg/";
    public static final String ICEBERG_DATABASE = "test";
    public static final String ICEBERG_TABLE = "user_features";

    // ==================== TiDB ====================
    public static final String TIDB_JDBC_URL = "jdbc:mysql://gateway01.us-east-1.prod.aws.tidbcloud.com:4000/test?useSSL=true&requireSSL=true&verifyServerCertificate=false";
    public static final String TIDB_USERNAME = "2F8Cw8v9xWqr9Pp.root";
    public static final String TIDB_PASSWORD = "C74gO8cF7wcVCPVj";
    public static final String TIDB_TABLE = "user_feature";
    public static final String TIDB_STATE_TABLE = "feature_state";

    // ==================== Flink ====================
    public static final long CHECKPOINT_INTERVAL_MS = 10_000;       // 检查点间隔：10 秒
    public static final long CHECKPOINT_MIN_PAUSE_MS = 5_000;       // 两次检查点最小间隔：5 秒
    public static final long CHECKPOINT_TIMEOUT_MS = 60_000;        // 检查点超时：60 秒

    // ==================== 作业名称 ====================
    public static final String RISK_JOB_NAME = "FeatureStore-RiskControl";
    public static final String RECOMMEND_JOB_NAME = "FeatureStore-Recommend";

    // ==================== 特征计算 ====================
    public static final int ROLLING_WINDOW_HOURS = 216;  // 9 天
    public static final int RECENT_HOURS_72H = 72;       // 3 天
}
