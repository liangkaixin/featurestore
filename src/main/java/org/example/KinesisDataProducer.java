package org.example;

import software.amazon.awssdk.core.SdkBytes;
import software.amazon.awssdk.services.kinesis.KinesisClient;
import software.amazon.awssdk.services.kinesis.model.PutRecordsRequest;
import software.amazon.awssdk.services.kinesis.model.PutRecordsRequestEntry;

import java.nio.charset.StandardCharsets;
import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import java.util.Random;
import java.util.UUID;

/**
 * 往 Kinesis test 流写入模拟 DMS JSON 数据，供 Flink 消费端调试使用。
 * 覆盖四张表：user_event / user_deposit / user_withdraw / user_play
 * 使用 PutRecords 批量写入（每批 500 条），目标每秒 1000 条。
 * 直接运行 main 方法即可，Ctrl+C 停止。
 */
public class KinesisDataProducer {

    private static final String STREAM_NAME = "test";
    private static final String[] TABLES = {"user_event", "user_deposit", "user_withdraw", "user_play"};
    private static final String[] EVENT_TYPES = {"purchase", "refund", "login", "click", "signup"};

    private static final int BATCH_SIZE = 150;   // PutRecords 单次上限 500 条
    private static final int TARGET_PER_SECOND = 300;
    private static final int BATCHES_PER_SECOND = TARGET_PER_SECOND / BATCH_SIZE;  // 2 批/秒

    public static void main(String[] args) {
        KinesisClient client = KinesisClient.create();
        Random random = new Random();
        long id = 1;

        System.out.println("开始往 Kinesis 流 [" + STREAM_NAME + "] 写入数据，目标 " + TARGET_PER_SECOND + " 条/秒，Ctrl+C 停止...");

        while (true) {
            long batchStart = System.currentTimeMillis();

            for (int b = 0; b < BATCHES_PER_SECOND; b++) {
                List<PutRecordsRequestEntry> entries = new ArrayList<>(BATCH_SIZE);

                for (int i = 0; i < BATCH_SIZE; i++) {
                    String table = TABLES[random.nextInt(TABLES.length)];
                    String json = buildRecord(id, table, random);

                    entries.add(PutRecordsRequestEntry.builder()
                            .partitionKey(UUID.randomUUID().toString())
                            .data(SdkBytes.fromString(json, StandardCharsets.UTF_8))
                            .build());
                    id++;
                }

                PutRecordsRequest request = PutRecordsRequest.builder()
                        .streamName(STREAM_NAME)
                        .records(entries)
                        .build();

                var response = client.putRecords(request);
                int failed = response.failedRecordCount();
                if (failed > 0) {
                    System.err.println("[批次] 失败 " + failed + "/" + BATCH_SIZE + " 条");
                }
            }

            long elapsed = System.currentTimeMillis() - batchStart;
            System.out.println("[" + id + "条] 本秒写入完成，耗时 " + elapsed + "ms");

            long remaining = 1000 - elapsed;
            if (remaining > 0) {
                try {
                    Thread.sleep(remaining);
                } catch (InterruptedException e) {
                    break;
                }
            }
        }

        client.close();
        System.out.println("生产者已停止，共写入 " + (id - 1) + " 条");
    }

    private static String buildRecord(long id, String table, Random random) {
        long userId = 1000 + random.nextInt(9000);
        String ts = Instant.now().toString();

        switch (table) {
            case "user_event":
                return String.format(
                        "{\"data\":{\"id\":%d,\"uid\":%d,\"user_id\":%d,\"event_type\":\"%s\",\"amount\":%.2f,"
                                + "\"entry_fee\":0,\"reward\":0,\"event_time\":\"%s\",\"created_at\":\"%s\"},"
                                + "\"metadata\":{\"operation\":\"insert\",\"table-name\":\"user_event\"}}",
                        id, userId, userId,
                        EVENT_TYPES[random.nextInt(EVENT_TYPES.length)],
                        random.nextDouble() * 1000,
                        ts, ts
                );

            case "user_deposit":
                return String.format(
                        "{\"data\":{\"id\":%d,\"uid\":%d,\"user_id\":%d,\"event_type\":null,\"amount\":%.2f,"
                                + "\"entry_fee\":0,\"reward\":0,\"event_time\":null,\"created_at\":\"%s\"},"
                                + "\"metadata\":{\"operation\":\"insert\",\"table-name\":\"user_deposit\"}}",
                        id, userId, userId,
                        50 + random.nextDouble() * 500,
                        ts
                );

            case "user_withdraw":
                return String.format(
                        "{\"data\":{\"id\":%d,\"uid\":%d,\"user_id\":%d,\"event_type\":null,\"amount\":%.2f,"
                                + "\"entry_fee\":0,\"reward\":0,\"event_time\":null,\"created_at\":\"%s\"},"
                                + "\"metadata\":{\"operation\":\"insert\",\"table-name\":\"user_withdraw\"}}",
                        id, userId, userId,
                        20 + random.nextDouble() * 200,
                        ts
                );

            case "user_play":
                return String.format(
                        "{\"data\":{\"id\":%d,\"uid\":%d,\"user_id\":%d,\"event_type\":null,\"amount\":0,"
                                + "\"entry_fee\":%.2f,\"reward\":%.2f,\"event_time\":null,\"created_at\":\"%s\"},"
                                + "\"metadata\":{\"operation\":\"insert\",\"table-name\":\"user_play\"}}",
                        id, userId, userId,
                        5 + random.nextDouble() * 50,
                        random.nextDouble() * 200,
                        ts
                );

            default:
                throw new IllegalArgumentException("Unknown table: " + table);
        }
    }
}
