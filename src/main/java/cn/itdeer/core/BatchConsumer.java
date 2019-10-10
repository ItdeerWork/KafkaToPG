package cn.itdeer.core;

import cn.itdeer.common.Constants;
import cn.itdeer.common.TopicToTable;
import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;

import java.sql.Connection;
import java.sql.SQLException;
import java.sql.Statement;

/**
 * Description : Batch类型写入
 * PackageName : cn.itdeer.core
 * ProjectName : KafkaToPG
 * CreatorName : itdeer.cn
 * CreateTime : 2019/8/16/10:32
 */
@Slf4j
public class BatchConsumer extends Thread {

    private KafkaConsumer<String, String> consumer;
    private TopicToTable ttt;
    private String[] fields = null;

    private Connection connection;
    private Statement stmt;

    /**
     * 构造函数 父类进行实例化，这里直接可以使用
     *
     * @param consumer   消费者实例
     * @param ttt        数据流向配置实例
     * @param fields     字段列表实例
     * @param connection 数据库连接实例
     * @param stmt       SQL执行器实例
     */
    public BatchConsumer(KafkaConsumer<String, String> consumer, TopicToTable ttt, String[] fields, Connection connection, Statement stmt) {
        this.consumer = consumer;
        this.ttt = ttt;
        this.fields = fields;
        this.connection = connection;
        this.stmt = stmt;
        addShutdownHook();
    }

    /**
     * 覆盖父类继承的线程类的启动方法
     */
    @Override
    public void run() {

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

        int number = 0;
        String insertSql = sqlPrefix;
        while (true) {
            try {
                ConsumerRecords<String, String> records = consumer.poll(100);
                for (ConsumerRecord<String, String> record : records) {
                    try {
                        JSONObject jsonObject = JSON.parseObject(record.value());

                        insertSql = insertSql + " (";
                        for (String filed : fields) {
                            insertSql = insertSql + "'" + jsonObject.get(filed) + "',";
                        }
                        insertSql = insertSql.substring(0, insertSql.length() - 1) + ") ";
                        stmt.addBatch(insertSql);
                        number++;
                        insertSql = sqlPrefix;
                    } catch (Exception e) {
                        log.error("Insert mode is [batch], Kafka data format is [json], An error occurred while parsing [{}] data. The error information is as follows:", record.value(), e.getStackTrace());
                    }
                }
                number = inputBatch(connection, stmt, batchSize, number);
            } catch (SQLException e) {
                log.error("Parsing kafka json format data to write data to postgresql error message is as follows:[{}]", e.getStackTrace());
                log.error("The data that caused the error is:[{}]", insertSql);
            }
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

        int number = 0;
        String insertSql = sqlPrefix;
        String separator = ttt.getOutputData().getSeparator() == null ? Constants.COMMA : ttt.getOutputData().getSeparator();
        while (true) {
            try {
                ConsumerRecords<String, String> records = consumer.poll(100);
                for (ConsumerRecord<String, String> record : records) {
                    try {
                        String[] values = record.value().split(separator);
                        insertSql = insertSql + " (";

                        for (int i = 0; i < fields.length; i++) {
                            insertSql = insertSql + "'" + values[i] + "',";
                        }
                        insertSql = insertSql.substring(0, insertSql.length() - 1) + ") ";
                        stmt.addBatch(insertSql);
                        number++;
                        insertSql = sqlPrefix;
                    } catch (Exception e) {
                        log.error("Insert mode is [batch], Kafka data format is [csv], An error occurred while parsing [{}] data. The error information is as follows:", record.value(), e.getStackTrace());
                    }
                }
                number = inputBatch(connection, stmt, batchSize, number);
            } catch (SQLException e) {
                log.error("Parsing kafka csv format data to write data to postgresql error message is as follows:[{}]", e.getStackTrace());
                log.error("The data that caused the error is:[{}]", insertSql);
            }
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

    /**
     * 注册一个停止运行的资源清理任务(钩子程序)
     */
    private void addShutdownHook() {
        Runtime.getRuntime().addShutdownHook(new Thread() {
            public void run() {
                close();
            }
        });
    }

    /**
     * 关闭资源
     */
    private void close() {
        try {
            if (stmt != null) {
                stmt.close();
            }
            if (connection != null) {
                connection.close();
            }
            if (fields != null) {
                fields = null;
            }
            if (ttt != null) {
                ttt = null;
            }
            if (consumer != null) {
                consumer.close();
            }
        } catch (SQLException e) {
            log.error("The closing resource error message is as follows: [{}]", e.getStackTrace());
        }
    }
}
