package org.example;

import org.example.config.AppConfig;

import java.math.BigDecimal;
import java.math.RoundingMode;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.Timestamp;
import java.time.Instant;
import java.util.Random;

/**
 * 往 AWS RDS MySQL 的 solitaire 库持续写入 mock 数据，模拟数据源头，
 * 用于验证 MySQL→Firehose/DMS→Kinesis→Flink 全链路。
 * 覆盖四张表：user_event / user_deposit / user_withdraw / user_play，轮询均匀分布。
 * 数据语义与 KinesisDataProducer 完全一致（金额区间、event_type、时间字段对齐）。
 * JDBC batch 每批 500 条，目标每秒 1000 条，直接运行 main 方法即可，Ctrl+C 停止。
 * <p>
 * 运行前需在 AppConfig.MYSQL_PASSWORD 填入 solitaire 库密码。
 */
public class MySqlDataProducer {

    private static final String[] TABLES = {"user_event", "user_deposit", "user_withdraw", "user_play"};
    private static final String[] EVENT_TYPES = {"purchase", "refund", "login", "click", "signup"};

    private static final int BATCH_SIZE = 50;
    private static final int TARGET_PER_SECOND = 100;
    private static final int BATCHES_PER_SECOND = TARGET_PER_SECOND / BATCH_SIZE;  // 2 批/秒

    // 各表 INSERT：id 为 AUTO_INCREMENT，created_at/updated_at 走默认值，均无需显式写入
    private static final String SQL_USER_EVENT =
            "INSERT INTO user_event (uid, event_type, amount, event_time) VALUES (?, ?, ?, ?)";
    private static final String SQL_USER_DEPOSIT =
            "INSERT INTO user_deposit (user_id, amount) VALUES (?, ?)";
    private static final String SQL_USER_WITHDRAW =
            "INSERT INTO user_withdraw (user_id, amount) VALUES (?, ?)";
    private static final String SQL_USER_PLAY =
            "INSERT INTO user_play (user_id, entry_fee, reward) VALUES (?, ?, ?)";

    private static volatile long totalWritten = 0;

    public static void main(String[] args) throws Exception {
        Random random = new Random();

        // Ctrl+C 优雅停止，打印总写入量
        Runtime.getRuntime().addShutdownHook(new Thread(() ->
                System.out.println("生产者已停止，共写入 " + totalWritten + " 条")));

        try (Connection conn = DriverManager.getConnection(
                AppConfig.MYSQL_JDBC_URL, AppConfig.MYSQL_USERNAME, AppConfig.MYSQL_PASSWORD)) {
            conn.setAutoCommit(false);

            try (PreparedStatement psEvent = conn.prepareStatement(SQL_USER_EVENT);
                 PreparedStatement psDeposit = conn.prepareStatement(SQL_USER_DEPOSIT);
                 PreparedStatement psWithdraw = conn.prepareStatement(SQL_USER_WITHDRAW);
                 PreparedStatement psPlay = conn.prepareStatement(SQL_USER_PLAY)) {

                System.out.println("开始往 solitaire 库写入数据，目标 " + TARGET_PER_SECOND + " 条/秒，Ctrl+C 停止...");

                int tableIdx = 0;   // 轮询四张表，保证均匀分布（500 可被 4 整除，每批各 125 条）
                while (true) {
                    long batchStart = System.currentTimeMillis();

                    for (int b = 0; b < BATCHES_PER_SECOND; b++) {
                        for (int i = 0; i < BATCH_SIZE; i++) {
                            String table = TABLES[tableIdx % TABLES.length];
                            tableIdx++;
                            fill(table, psEvent, psDeposit, psWithdraw, psPlay, random);
                        }
                        flush(conn, psEvent, psDeposit, psWithdraw, psPlay);
                        totalWritten += BATCH_SIZE;
                    }

                    long elapsed = System.currentTimeMillis() - batchStart;
                    System.out.println("[" + totalWritten + "条] 本秒写入完成，耗时 " + elapsed + "ms");

                    long remaining = 1000 - elapsed;
                    if (remaining > 0) {
                        Thread.sleep(remaining);
                    }
                }
            }
        }
    }

    /** 按表名把一条 mock 数据填入对应的 PreparedStatement 批次。 */
    private static void fill(String table, PreparedStatement psEvent, PreparedStatement psDeposit,
                             PreparedStatement psWithdraw, PreparedStatement psPlay, Random random)
            throws Exception {
        long userId = 1000 + random.nextInt(9000);

        switch (table) {
            case "user_event":
                psEvent.setLong(1, userId);
                psEvent.setString(2, EVENT_TYPES[random.nextInt(EVENT_TYPES.length)]);
                psEvent.setBigDecimal(3, amount(random.nextDouble() * 1000));
                psEvent.setTimestamp(4, Timestamp.from(Instant.now()));
                psEvent.addBatch();
                break;
            case "user_deposit":
                psDeposit.setLong(1, userId);
                psDeposit.setBigDecimal(2, amount(50 + random.nextDouble() * 500));
                psDeposit.addBatch();
                break;
            case "user_withdraw":
                psWithdraw.setLong(1, userId);
                psWithdraw.setBigDecimal(2, amount(20 + random.nextDouble() * 200));
                psWithdraw.addBatch();
                break;
            case "user_play":
                psPlay.setLong(1, userId);
                psPlay.setBigDecimal(2, amount(5 + random.nextDouble() * 50));
                psPlay.setBigDecimal(3, amount(random.nextDouble() * 200));
                psPlay.addBatch();
                break;
            default:
                throw new IllegalArgumentException("Unknown table: " + table);
        }
    }

    /** 提交本批：执行四张表的 batch 并统一 commit。 */
    private static void flush(Connection conn, PreparedStatement psEvent, PreparedStatement psDeposit,
                              PreparedStatement psWithdraw, PreparedStatement psPlay) throws Exception {
        psEvent.executeBatch();
        psDeposit.executeBatch();
        psWithdraw.executeBatch();
        psPlay.executeBatch();
        conn.commit();
    }

    /** 金额保留两位小数，对齐 decimal(20,2)。 */
    private static BigDecimal amount(double v) {
        return BigDecimal.valueOf(v).setScale(2, RoundingMode.HALF_UP);
    }
}
