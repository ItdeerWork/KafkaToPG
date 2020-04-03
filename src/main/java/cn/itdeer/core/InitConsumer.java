package cn.itdeer.core;

import cn.itdeer.common.InitConfig;
import cn.itdeer.common.Kafka;
import cn.itdeer.common.TopicToTable;
import com.alibaba.druid.pool.DruidDataSource;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.PartitionInfo;

import java.util.Arrays;
import java.util.List;
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
public class InitConsumer {

    private KafkaConsumer<String, String> consumer;
    private DruidDataSource dds;

    /**
     * 构造函数 初始化Consumer的实例
     *
     * @param kafka Kafka消费之的基础信息
     * @param ttt   从Kafka到表的配置信息
     */
    public InitConsumer(Kafka kafka, TopicToTable ttt) {

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
            new CopyConsumer(consumer, ttt, dds).start();
        } catch (Exception e) {
            log.error("Error retrieving connection from connection pool or instantiating processing instance. Error message:[{}]", e.getStackTrace());
        }
    }
}
