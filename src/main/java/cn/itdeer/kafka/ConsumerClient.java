package cn.itdeer.kafka;

import cn.itdeer.common.*;
import com.alibaba.druid.pool.DruidDataSource;
import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;

import java.sql.Connection;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.*;

/**
 * Description : Kafka消费者
 * PackageName : cn.itdeer.kafka
 * ProjectName : KafkaToPG
 * CreatorName : itdeer.cn
 * CreateTime : 2019/8/14/9:26
 */

@Slf4j
public class ConsumerClient extends Thread {

    private final KafkaConsumer<String, String> consumer;
    private DruidDataSource dds;
    private TopicToTable ttt;
    private String[] fields = null;

    private Connection connection;
    private Statement stmt;

    /**
     * 构造函数
     *
     * @param kafka Kafka消费之的基础信息
     * @param ttt   从Kafka到表的配置信息
     */
    public ConsumerClient(Kafka kafka, TopicToTable ttt, String[] fields) {
        Properties properties = new Properties();
        properties.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, ttt.getOutputData().getBootstrapServers());
        properties.put(ConsumerConfig.GROUP_ID_CONFIG, ttt.getOutputData().getTopicName() + "_" + kafka.getGroupIdSuffix());
        properties.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, kafka.getKeyDeserializerClass());
        properties.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, kafka.getValueDeserializerClass());
        properties.put(ConsumerConfig.AUTO_COMMIT_INTERVAL_MS_CONFIG, kafka.getAutoCommitIntervalMs());
        properties.put(ConsumerConfig.MAX_POLL_RECORDS_CONFIG, kafka.getMaxPollRecords());
        properties.put(ConsumerConfig.FETCH_MAX_BYTES_CONFIG, kafka.getFetchMaxBytes());
        properties.put(ConsumerConfig.FETCH_MAX_WAIT_MS_CONFIG, kafka.getFetchMaxWaitMs());
        properties.put(ConsumerConfig.SESSION_TIMEOUT_MS_CONFIG, kafka.getSessionTimeoutMs());
        properties.put(ConsumerConfig.HEARTBEAT_INTERVAL_MS_CONFIG, kafka.getHeartbeatIntervalMs());
        properties.put(ConsumerConfig.MAX_POLL_INTERVAL_MS_CONFIG, kafka.getMaxPollIntervalMs());
        properties.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, kafka.getAutoOffsetReset());
        properties.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, kafka.getEnableAutoCommit());

        consumer = new KafkaConsumer(properties);
        consumer.subscribe(Arrays.asList(ttt.getOutputData().getTopicName()));

        Map<String, DruidDataSource> map = InitConfig.getConnectionMap();
        dds = map.get(ttt.getInputData().getTable());

        this.ttt = ttt;
        this.fields = fields;
    }

    /**
     * 覆盖父类run方法
     */
    @Override
    public void run() {
        try {
            connection = dds.getConnection();
            connection.setAutoCommit(false);
            stmt = connection.createStatement();

            /**
             * 准备插入SQL的前半部分
             */
            String sqlPrefix = "INSERT INTO \"" + ttt.getInputData().getTable() + "\"(";
            for (String filed : fields) {
                sqlPrefix = sqlPrefix + filed + ",";
            }
            sqlPrefix = sqlPrefix.substring(0, sqlPrefix.length() - 1) + ") VALUES";

            /**
             * 接收数据 组装完整的插入语句
             */
            int batchSize = ttt.getCommons().getBatchSize();
            switch (ttt.getOutputData().getFormat()) {
                case Constants.JSON:
                    jsonData(connection, stmt, sqlPrefix, batchSize);
                case Constants.CSV:
                    csvData(connection, stmt, sqlPrefix, batchSize);
                default:
                    log.error("The data format you set is not currently supported, only JSON and CSV are supported");
            }
        } catch (SQLException e) {
            e.printStackTrace();
        }
    }

    /**
     * 数据为JSON格式
     *
     * @param connection 数据库连接
     * @param stmt       Statement对象
     * @param sqlPrefix  SQL的前半部分
     * @param batchSize  批处理大小
     */
    private void jsonData(Connection connection, Statement stmt, String sqlPrefix, int batchSize) {
        try {
            int number = 0;
            String insertSql = sqlPrefix;
            while (true) {
                ConsumerRecords<String, String> records = consumer.poll(100);
                for (ConsumerRecord<String, String> record : records) {
                    JSONObject jsonObject = JSON.parseObject(record.value());

                    insertSql = insertSql + " (";
                    for (String filed : fields) {
                        insertSql = insertSql + "'" + jsonObject.get(filed) + "',";
                    }
                    insertSql = insertSql.substring(0, insertSql.length() - 1) + ") ";
                    stmt.addBatch(insertSql);
                    number++;
                    insertSql = sqlPrefix;
                }
                number = inputBatch(connection, stmt, batchSize, number);
            }
        } catch (SQLException e) {
            log.error("Parsing kafka json format data to write data to postgresql error message is as follows:[{}]", e.getStackTrace());
        }
    }

    /**
     * 数据为CSV格式
     *
     * @param connection 数据库连接
     * @param stmt       Statement对象
     * @param sqlPrefix  SQL的前半部分
     * @param batchSize  批处理大小
     */
    private void csvData(Connection connection, Statement stmt, String sqlPrefix, int batchSize) {
        try {
            int number = 0;
            String insertSql = sqlPrefix;
            String separator = ttt.getOutputData().getSeparator();
            while (true) {
                ConsumerRecords<String, String> records = consumer.poll(100);
                for (ConsumerRecord<String, String> record : records) {
                    String[] values = record.value().split(separator);
                    insertSql = insertSql + " (";

                    for (int i = 0; i < fields.length; i++) {
                        insertSql = insertSql + "'" + values[i] + "',";
                    }
                    insertSql = insertSql.substring(0, insertSql.length() - 1) + ") ";
                    stmt.addBatch(insertSql);
                    number++;
                    insertSql = sqlPrefix;
                }
                number = inputBatch(connection, stmt, batchSize, number);
            }
        } catch (SQLException e) {
            log.error("Parsing kafka csv format data to write data to postgresql error message is as follows:[{}]", e.getStackTrace());
        }
    }

    /**
     * 插入操作
     *
     * @param connection 数据库连接
     * @param stmt       Statement对象
     * @param batchSize  批处理大小
     * @param number     当前处理条数
     * @return
     * @throws SQLException
     */
    private int inputBatch(Connection connection, Statement stmt, int batchSize, int number) throws SQLException {
        if (number >= batchSize) {
            stmt.executeBatch();
            connection.commit();
            stmt.clearBatch();
            consumer.commitSync();
            number = 0;
        }
        return number;
    }
}
