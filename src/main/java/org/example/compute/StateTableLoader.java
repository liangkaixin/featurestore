package org.example.compute;

import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.zaxxer.hikari.HikariConfig;
import com.zaxxer.hikari.HikariDataSource;
import org.example.config.AppConfig;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.util.HashMap;
import java.util.Map;

/**
 * 从 TiDB feature_state 表加载 Flink 状态的初始值。
 * 用于回溯/修正场景：SQL 先算出正确值写入状态表，Flink 启动时从状态表加载，实现状态衔接。
 *
 * 内部使用 HikariCP 连接池复用连接，避免热路径中逐条创建 JDBC 连接。
 * 生命周期由调用方管理：open() 初始化连接池，close() 释放资源。
 */
public class StateTableLoader {

    private static final ObjectMapper MAPPER = new ObjectMapper();
    private static final String LOAD_SQL =
            "SELECT state_value FROM " + AppConfig.TIDB_STATE_TABLE + " WHERE uid = ? AND state_key = ?";

    private transient HikariDataSource dataSource;

    /**
     * 初始化连接池。在 Flink 算子 open() 中调用。
     */
    public void open() {
        HikariConfig config = new HikariConfig();
        config.setJdbcUrl(AppConfig.TIDB_JDBC_URL);
        config.setUsername(AppConfig.TIDB_USERNAME);
        config.setPassword(AppConfig.TIDB_PASSWORD);
        config.setMaximumPoolSize(4);
        config.setMinimumIdle(1);
        config.setConnectionTimeout(10_000);
        dataSource = new HikariDataSource(config);
    }

    /**
     * 关闭连接池。在 Flink 算子 close() 中调用。
     */
    public void close() {
        if (dataSource != null && !dataSource.isClosed()) {
            dataSource.close();
        }
    }

    /**
     * 加载单个 double 值状态（如 bankruptcy_protection）。
     *
     * @param uid      用户 ID
     * @param stateKey 状态键（如 "bankruptcy_protection"）
     * @return 状态值，不存在时返回 null
     */
    public Double loadDouble(long uid, String stateKey) {
        try (Connection conn = dataSource.getConnection();
             PreparedStatement ps = conn.prepareStatement(LOAD_SQL)) {
            ps.setLong(1, uid);
            ps.setString(2, stateKey);
            ResultSet rs = ps.executeQuery();
            if (rs.next()) {
                return Double.parseDouble(rs.getString("state_value"));
            }
        } catch (Exception e) {
            // 状态表不存在或查询失败，返回 null（首次用户）
        }
        return null;
    }

    /**
     * 加载 double 数组状态（如 lifetime [total_amount, event_count]）。
     *
     * @param uid      用户 ID
     * @param stateKey 状态键（如 "lifetime"）
     * @return 状态数组，不存在时返回 null
     */
    public double[] loadDoubleArray(long uid, String stateKey) {
        try (Connection conn = dataSource.getConnection();
             PreparedStatement ps = conn.prepareStatement(LOAD_SQL)) {
            ps.setLong(1, uid);
            ps.setString(2, stateKey);
            ResultSet rs = ps.executeQuery();
            if (rs.next()) {
                String json = rs.getString("state_value");
                return MAPPER.readValue(json, double[].class);
            }
        } catch (Exception e) {
            // 状态表不存在或查询失败，返回 null
        }
        return null;
    }

    /**
     * 加载 Map 状态（如 rolling {hourEpoch → [game_count, gtv_sum]}）。
     *
     * @param uid      用户 ID
     * @param stateKey 状态键（如 "rolling"）
     * @return 状态 Map，不存在时返回 null
     */
    public Map<Long, double[]> loadMap(long uid, String stateKey) {
        try (Connection conn = dataSource.getConnection();
             PreparedStatement ps = conn.prepareStatement(LOAD_SQL)) {
            ps.setLong(1, uid);
            ps.setString(2, stateKey);
            ResultSet rs = ps.executeQuery();
            if (rs.next()) {
                String json = rs.getString("state_value");
                // JSON: {"1696838400": [10.0, 20.0], ...}
                Map<String, double[]> stringKeyMap = MAPPER.readValue(json,
                        new TypeReference<HashMap<String, double[]>>() {});
                Map<Long, double[]> result = new HashMap<>();
                for (Map.Entry<String, double[]> entry : stringKeyMap.entrySet()) {
                    result.put(Long.parseLong(entry.getKey()), entry.getValue());
                }
                return result;
            }
        } catch (Exception e) {
            // 状态表不存在或查询失败，返回 null
        }
        return null;
    }
}
