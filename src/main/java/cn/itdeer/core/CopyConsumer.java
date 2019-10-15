package cn.itdeer.core;

import cn.itdeer.common.Constants;
import cn.itdeer.common.TopicToTable;
import com.alibaba.druid.pool.DruidDataSource;
import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.postgresql.copy.CopyManager;
import org.postgresql.core.BaseConnection;

import java.io.ByteArrayInputStream;
import java.sql.Connection;
import java.sql.SQLException;

/**
 * Description : Copy类型写入
 * PackageName : cn.itdeer.core
 * ProjectName : KafkaToPG
 * CreatorName : itdeer.cn
 * CreateTime : 2019/8/16/10:32
 */
@Slf4j
public class CopyConsumer extends Thread {

    private KafkaConsumer<String, String> consumer;
    private TopicToTable ttt;
    private String[] fields = null;

    private BaseConnection baseConn;
    private CopyManager copyManager = null;
    private StringBuffer sb;
    private DruidDataSource dds;

    /**
     * 构造函数 父类进行实例化，这里直接可以使用
     *
     * @param consumer 消费者实例
     * @param ttt      数据流向配置实例
     * @param fields   字段列表实例
     * @param dds      连接池信息
     */
    public CopyConsumer(KafkaConsumer<String, String> consumer, TopicToTable ttt, String[] fields, DruidDataSource dds) {
        this.consumer = consumer;
        this.ttt = ttt;
        this.fields = fields.clone();
        this.dds = dds;
        init();
        sb = new StringBuffer(1000);
        addShutdownHook();
    }

    /**
     * 初始化连接信息
     *
     * @return CopyManager 通道管理实例
     */
    private CopyManager init() {
        if (copyManager == null) {
            Connection connection;
            try {
                connection = dds.getConnection();
                connection.setAutoCommit(false);
                baseConn = (BaseConnection) connection.getMetaData().getConnection();
                copyManager = new CopyManager(baseConn);
            } catch (Exception e) {
                log.error("Error retrieving connection from connection pool or instantiating processing instance. Error message:[{}]", e.getStackTrace());
            }
        }
        return copyManager;
    }

    /**
     * 覆盖线程run方法
     */
    @Override
    public void run() {
        switch (ttt.getOutputData().getFormat().toUpperCase()) {
            case Constants.JSON:
                jsonData();
                break;
            case Constants.CSV:
                csvData();
                break;
            default:
                log.error("The data format you set is not currently supported, only JSON and CSV are supported");
                break;
        }
    }

    /**
     * 数据为JSON格式
     */
    private void jsonData() {
        int field_size = fields.length - 1;
        while (true) {
            try {
                ConsumerRecords<String, String> records = consumer.poll(100);
                for (ConsumerRecord<String, String> record : records) {
                    try {
                        JSONObject jsonObject = JSON.parseObject(record.value());
                        for (int i = 0; i < field_size; i++) {
                            sb.append(jsonObject.get(fields[i]).toString() + ",");
                        }
                        sb.append(jsonObject.get(fields[field_size]).toString() + "\n");
                    } catch (Exception e) {
                        log.error("Insert mode is [copy], Kafka data format is [json], An error occurred while parsing [{}] data. The error information is as follows:[{}]", record.value(), e.getStackTrace());
                    }
                }

                if (sb.length() > 0) {
                    if (copyManager == null) {
                        init();
                    }
                    copyManager.copyIn("COPY " + ttt.getInputData().getTable() + " FROM STDIN USING DELIMITERS ','", new ByteArrayInputStream(sb.toString().getBytes("UTF-8")));
                    baseConn.commit();
                    consumer.commitAsync();
                    sb.setLength(0);
                    System.out.println("发送成功");
                }
            } catch (Exception e) {
                log.error("Parsing kafka json format data to write data to postgresql error message is as follows:[{}]", e.getStackTrace());
                log.error("The data that caused the error is:[{}]", sb.toString());
                sb.setLength(0);
            }
        }
    }

    /**
     * 数据为CSV格式
     */
    private void csvData() {
        while (true) {
            ConsumerRecords<String, String> records = consumer.poll(100);
            for (ConsumerRecord<String, String> record : records) {
                try {
                    sb.append(record.value() + "\n");
                } catch (Exception e) {
                    log.error("Insert mode is [copy], Kafka data format is [json], An error occurred while parsing [{}] data. The error information is as follows:[{}]", record.value(), e.getStackTrace());
                }
            }
            if (sb.length() > 0) {
                try {
                    if (copyManager == null) {
                        init();
                    }
                    copyManager.copyIn("COPY " + ttt.getInputData().getTable() + " FROM STDIN USING DELIMITERS '" + ttt.getOutputData().getSeparator() + "'", new ByteArrayInputStream(sb.toString().getBytes("UTF-8")));
                    baseConn.commit();
                    consumer.commitAsync();
                    sb.setLength(0);
                } catch (Exception e) {
                    log.error("Parsing kafka csv format data to write data to postgresql error message is as follows:[{}]", e.getStackTrace());
                    sb.setLength(0);
                }
            }
        }
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
            if (sb != null) {
                sb = null;
            }
            if (copyManager != null) {
                copyManager = null;
            }
            if (baseConn != null) {
                baseConn.close();
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
