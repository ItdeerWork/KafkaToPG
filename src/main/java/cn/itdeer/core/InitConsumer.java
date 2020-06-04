package cn.itdeer.core;

import cn.itdeer.common.Kafka;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

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
public class InitConsumer {

    private static final Logger log = LogManager.getLogger(DataOperate.class);

    private KafkaConsumer<String, String> consumer;

    /**
     * 构造函数 初始化Consumer的实例
     *
     * @param kafka Kafka消费之的基础信息
     * @param topicName    从Kafka到表的配置信息
     * @param map   从Kafka到表的配置信息
     */
    public InitConsumer(Kafka kafka, String topicName, Map<String, String> map) {

        Properties properties = new Properties();
        properties.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, kafka.getBootstrapServers());
        properties.put(ConsumerConfig.GROUP_ID_CONFIG, topicName + "_" + kafka.getGroupIdSuffix());
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
        consumer.subscribe(Arrays.asList(topicName));

        try {
            new CopyConsumer(consumer, map).start();
        } catch (Exception e) {
            log.error("An error occurred starting a CopyConsumer to start work. Error message:[{}]", e.getStackTrace());
        }
    }

}
