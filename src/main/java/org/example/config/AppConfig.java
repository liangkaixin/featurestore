package org.example.config;

import java.net.InetAddress;
import java.net.UnknownHostException;

/**
 * 应用配置常量。
 * 生产环境建议从环境变量或配置中心读取。
 */
public final class AppConfig {

    private AppConfig() {}

    // ==================== AWS ====================
    public static final String AWS_REGION = "us-east-1";
    public static final String KINESIS_STREAM_NAME = "test";
    public static final String EFO_CONSUMER_ARN = "arn:aws:kinesis:us-east-1:700526301352:stream/test/consumer/flink-efo-consumer:1791622821";

    // ==================== MySQL 源库 (AWS RDS / solitaire) ====================
    // rewriteBatchedStatements=true 让驱动把批量 INSERT 重写为 multi-value 语句，大幅提升写入吞吐
    public static final String MYSQL_JDBC_URL = "jdbc:mysql://database-1.cw3sqmwyelui.us-east-1.rds.amazonaws.com:3306/solitaire"
            + "?rewriteBatchedStatements=true&useSSL=false&allowPublicKeyRetrieval=true&serverTimezone=UTC";
    public static final String MYSQL_USERNAME = "admin";
    public static final String MYSQL_PASSWORD = "qq123123";

    // ==================== Iceberg ====================
    public static final String ICEBERG_CATALOG_NAME = "glue";
    public static final String ICEBERG_WAREHOUSE = "s3://feature-store-warehouse/iceberg/";
    public static final String ICEBERG_DATABASE = "test";
    public static final String ICEBERG_TABLE = "user_features";

    // ==================== TiDB ====================
    // 默认公网 gateway 地址（本地调试用，也是环境变量未设置时的回落值）
    private static final String DEFAULT_TIDB_JDBC_URL =
            "jdbc:mysql://gateway01.us-east-1.prod.aws.tidbcloud.com:4000/test?useSSL=true&requireSSL=true&verifyServerCertificate=false";

    // TiDB PrivateLink 私网域名：仅在 MSF VPC 内（含 VPC 内 CloudShell）经 VPC 私网 DNS 可解析；
    // 本地/普通 CloudShell 解析失败(NXDOMAIN)，故可用其作为环境探测依据。
    private static final String TIDB_PRIVATE_HOST = "gateway01-privatelink.us-east-1.prod.aws.tidbcloud.com";
    private static final String TIDB_PRIVATE_JDBC_URL =
            "jdbc:mysql://" + TIDB_PRIVATE_HOST + ":4000/test?useSSL=true&requireSSL=true&verifyServerCertificate=false";

    // 生产(MSF 接 VPC/PrivateLink)通过环境变量 TIDB_JDBC_URL 注入私网 URL（在 MSF 应用环境变量中配置）；
    // 未设置时按 DNS 探测自动选择：私网域名可解析→私网 URL；不可解析→公网 URL。
    // 同一 JAR 在 MSF 接 VPC 前（公网 egress，探测失败→公网）与接 VPC 后（探测成功→私网）均自动正确。
    public static final String TIDB_JDBC_URL = resolveTiDbJdbcUrl();
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

    /**
     * 解析 TiDB JDBC URL：环境变量 TIDB_JDBC_URL > DNS 探测（私网域名可解析→私网 URL）> 默认公网 URL。
     * 探测在 JVM 静态初始化时执行一次，结果缓存于常量。
     */
    private static String resolveTiDbJdbcUrl() {
        String env = System.getenv("TIDB_JDBC_URL");
        if (env != null && !env.trim().isEmpty()) {
            return env.trim();
        }
        try {
            InetAddress.getByName(TIDB_PRIVATE_HOST);
            return TIDB_PRIVATE_JDBC_URL;
        } catch (UnknownHostException e) {
            return DEFAULT_TIDB_JDBC_URL;
        }
    }
}
