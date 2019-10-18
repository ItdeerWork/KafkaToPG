package cn.itdeer.core;

import cn.itdeer.common.Constants;
import cn.itdeer.common.InitConfig;
import cn.itdeer.common.TopicToTable;
import com.alibaba.druid.pool.DruidDataSource;
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

    private volatile KafkaConsumer<String, String> consumer;
    private TopicToTable ttt;
    private String[] fields = null;

    private Connection connection;
    private Statement stmt;
    private DruidDataSource dds;

    /**
     * 构造函数 父类进行实例化，这里直接可以使用
     *
     * @param consumer 消费者实例
     * @param ttt      数据流向配置实例
     * @param fields   字段列表实例
     * @param dds      连接池信息
     */
    public BatchConsumer(KafkaConsumer<String, String> consumer, TopicToTable ttt, String[] fields, DruidDataSource dds) {
        this.consumer = consumer;
        this.ttt = ttt;
        this.fields = fields.clone();
        this.dds = dds;
        init();
    }

    /**
     * 初始化连接信息
     */
    private void init() {
        log.info("初始化连接资源......活动的连接个数：" + dds.getActiveCount());
        try {
            dds = InitConfig.getConnectionMap().get(ttt.getInputData().getTable());
            connection = dds.getConnection();
            connection.setAutoCommit(false);
            stmt = connection.createStatement();
        } catch (Exception e) {
            log.error("Error retrieving connection from connection pool or instantiating processing instance. Error message:[{}]", e.getStackTrace());
        }
    }

    /**
     * 覆盖父类继承的线程类的启动方法
     */
    @Override
    public void run() {

        /**
         * 准备插入SQL的前半部分
         */
        StringBuffer sqlPrefix = new StringBuffer();
        sqlPrefix.append("INSERT INTO \"").append(ttt.getInputData().getTable()).append("\"(");
        for (String filed : fields) {
            sqlPrefix.append(filed + ",");
        }
        String sqlPrefix_s = sqlPrefix.toString();
        sqlPrefix_s = sqlPrefix_s.substring(0, sqlPrefix_s.length() - 1) + ") VALUES";
        /**
         * 接收数据 组装完整的插入语句
         */
        int batchSize = ttt.getCommons().getBatchSize();
        switch (ttt.getOutputData().getFormat().toUpperCase()) {
            case Constants.JSON:
                jsonData(connection, stmt, sqlPrefix_s, batchSize);
                break;
            case Constants.CSV:
                csvData(connection, stmt, sqlPrefix_s, batchSize);
                break;
            default:
                log.error("The data format you set is not currently supported, only JSON and CSV are supported");
                break;
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
        try {
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
                    if (number == batchSize) {
                        number = inputBatch(connection, stmt, number);
                        consumer.commitAsync();
                    }
                    insertSql = sqlPrefix;
                }
            }
        } catch (Exception e) {
            log.error("Insert mode is [batch], Kafka data format is [json], An error occurred while parsing data. The error information is as follows:[{}]", e);
        } finally {
            try {
                consumer.commitSync();
                closeAll();
            } finally {
                consumer.close();
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

        try {
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
                    if (number == batchSize) {
                        number = inputBatch(connection, stmt, number);
                        consumer.commitAsync();
                    }
                    insertSql = sqlPrefix;
                }
            }
        } catch (Exception e) {
            log.error("Insert mode is [batch], Kafka data format is [csv], An error occurred while parsing data. The error information is as follows:[{}]", e);
        } finally {
            try {
                consumer.commitSync();
                closeAll();
            } finally {
                consumer.close();
            }
        }
    }

    /**
     * 插入操作
     *
     * @param connection 数据库连接
     * @param stmt       Statement对象
     * @param number     当前处理条数
     * @return
     * @throws SQLException
     */
    private int inputBatch(Connection connection, Statement stmt, int number) {
        try {
            if (stmt == null || connection == null || connection.isClosed())
                init();
            stmt.executeBatch();
            connection.commit();
            stmt.clearBatch();
            log.info("Use batch to successfully write a batch data to Postgresql database, the length is:[{}]", number);
            number = 0;
        } catch (SQLException e) {
            close();
            init();
        }
        return number;
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
        } catch (SQLException e) {
            log.error("The closing resource error message is as follows: [{}]", e.getStackTrace());
        }
    }

    /**
     * 关闭资源
     */
    private void closeAll() {
        close();
        if (fields != null) {
            fields = null;
        }
        if (ttt != null) {
            ttt = null;
        }
        if (consumer != null) {
            consumer.close();
        }
    }
}
