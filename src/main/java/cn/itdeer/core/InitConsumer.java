package cn.itdeer.core;

import cn.itdeer.common.Constants;
import cn.itdeer.common.InitConfig;
import cn.itdeer.common.Kafka;
import cn.itdeer.common.TopicToTable;
import com.alibaba.druid.pool.DruidDataSource;
import lombok.Data;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.KafkaConsumer;

import java.sql.Connection;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.Arrays;
import java.util.Map;
import java.util.Properties;

/**
 * Description : 初始化消费者及处理资源
 * PackageName : cn.itdeer.core
 * ProjectName : KafkaToPG
 * CreatorName : itdeer.cn
 * CreateTime : 2019/8/16/9:45
 */
@Slf4j
@Data
public class InitConsumer {

    private KafkaConsumer<String, String> consumer;
    private DruidDataSource dds;
    private TopicToTable ttt;
    private Kafka kafka;
    private String[] fields = null;

    private Connection connection;
    private Statement stmt;

    /**
     * 构造函数 初始化Consumer的实例
     *
     * @param kafka  Kafka消费之的基础信息
     * @param ttt    从Kafka到表的配置信息
     * @param fields 数据字段
     */
    public InitConsumer(Kafka kafka, TopicToTable ttt, String[] fields) {
        this.ttt = ttt;
        this.fields = fields;
        this.kafka = kafka;
        init(ttt.getCommons().getType());

        addShutdownHook();
    }

    /**
     * 初始化连接资源及相应的处理实例
     *
     * @param type 设置处理类型
     */
    public void init(String type) {

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

        try {
            log.info("Start initializing [{}] processing instance of type [{}]", type, type);

            switch (type) {
                case Constants.BATCH_TYPE:
                    new BatchConsumer(consumer, ttt, fields, dds).start();
                case Constants.COPY_TYPE:
                    new CopyConsumer(consumer, ttt, fields, dds).start();
            }
        } catch (Exception e) {
            log.error("Error retrieving connection from connection pool or instantiating processing instance. Error message:[{}]", e.getStackTrace());
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
            if (dds != null) {
                dds.close();
            }
            if (connection != null) {
                connection.close();
            }
            if (kafka != null) {
                kafka = null;
            }
        } catch (SQLException e) {
            log.error("The closing resource error message is as follows: [{}]", e.getStackTrace());
        }
    }
}
