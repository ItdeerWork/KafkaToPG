package cn.itdeer.core;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.Map;

/**
 * Description : 消费Kafka数据写入Map中
 * PackageName : cn.itdeer.core
 * ProjectName : KafkaToPG
 * CreatorName : itdeer.cn
 * CreateTime : 2019/8/16/10:32
 */
public class CopyConsumer extends Thread {

    private static final Logger log = LogManager.getLogger(CopyConsumer.class);

    private KafkaConsumer<String, String> consumer;
    private Map<String, String> map;


    /**
     * 构造函数 父类进行实例化，这里直接可以使用
     *
     * @param consumer 消费者实例
     * @param map      数据流向配置实例
     */
    public CopyConsumer(KafkaConsumer<String, String> consumer, Map<String, String> map) {
        this.consumer = consumer;
        this.map = map;
    }

    /**
     * 覆盖线程run方法
     */
    @Override
    public void run() {
        try {
            while (true) {
                ConsumerRecords<String, String> records = consumer.poll(100);
                try {
                    for (ConsumerRecord<String, String> record : records) {
                        JSONObject data = JSON.parseObject(record.value());
                        map.put(data.getString("tagName"), data.get("tagValue") + "," + data.get("piTS") + "," + data.get("sendTS"));
                    }
                } catch (Exception e) {
                    log.error("An error occurred while parsing json data. The error information is as follows:[{}]", e.getStackTrace());
                }
                consumer.commitAsync();
            }
        } catch (Exception e) {
            log.error("Parsing kafka json format data to write data to postgresql error message is as follows:[{}]", e);
        } finally {
            consumer.commitSync();
            consumer.close();
            map = null;
        }
    }

}
