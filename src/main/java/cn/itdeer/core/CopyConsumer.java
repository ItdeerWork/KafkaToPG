package cn.itdeer.core;

import cn.itdeer.common.InitConfig;
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
import java.sql.Statement;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.HashMap;
import java.util.Map;

/**
 * Description : Copy类型写入
 * PackageName : cn.itdeer.core
 * ProjectName : KafkaToPG
 * CreatorName : itdeer.cn
 * CreateTime : 2019/8/16/10:32
 */
@Slf4j
public class CopyConsumer extends Thread {

    public static final String DATA_FORMAT = "yyyy-MM-dd HH:mm:ss";
    public static final SimpleDateFormat format = new SimpleDateFormat(DATA_FORMAT);

    private KafkaConsumer<String, String> consumer;
    private TopicToTable ttt;

    private BaseConnection baseConn;
    private CopyManager copyManager = null;
    private StringBuffer sb;
    private DruidDataSource dds;
    private Connection connection;

    private Map<String, String> map;

    private String deleteSql;
    private Statement statement;

    /**
     * 构造函数 父类进行实例化，这里直接可以使用
     *
     * @param consumer 消费者实例
     * @param ttt      数据流向配置实例
     * @param dds      连接池信息
     */
    public CopyConsumer(KafkaConsumer<String, String> consumer, TopicToTable ttt, DruidDataSource dds) {
        this.consumer = consumer;
        this.ttt = ttt;
        this.dds = dds;
        map = new HashMap<>(500);
        deleteSql = "delete from " + ttt.getInputData().getTable() + " where sendts < ";

        init();
    }

    /**
     * 初始化连接信息
     *
     * @return CopyManager 通道管理实例
     */
    private CopyManager init() {
        if (copyManager == null || dds.isClosed()) {
            try {
                sb = new StringBuffer();
                dds = InitConfig.getConnectionMap().get(ttt.getInputData().getTable());
                connection = dds.getConnection();
                connection.setAutoCommit(false);
                baseConn = (BaseConnection) connection.getMetaData().getConnection();
                copyManager = new CopyManager(baseConn);

                statement = connection.createStatement();
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
        long endTime = System.currentTimeMillis() + ttt.getFrequency() * 1000;
        long durationEndTime = System.currentTimeMillis() - ttt.getDuration() * 3600000;

        try {
            while (true) {
                ConsumerRecords<String, String> records = consumer.poll(100);

                long nowTime = System.currentTimeMillis();

                for (ConsumerRecord<String, String> record : records) {
                    JSONObject data;
                    try {
                        data = JSON.parseObject(record.value());

                        boolean isGood = data.getBoolean("isGood");
                        if (!isGood) {
                            continue;
                        }

                        String tagName = data.getString("tagName");
                        Object tagValue = data.get("tagValue");
                        Object sendTS = data.get("sendTS");
                        Object piTS = data.get("piTS");
                        String message = tagValue + "," + isGood + "," + sendTS + "," + piTS;

                        map.put(tagName, message);
                    } catch (Exception e) {
                        log.error("Insert mode is [copy], Kafka data format is [json], An error occurred while parsing [{}] data. The error information is as follows:[{}]", record.value(), e.getStackTrace());
                    }
                }

                if (nowTime >= endTime) {
                    if (connection.isClosed())
                        init();
                    for (String key : map.keySet()) {
                        sb.append(key + ",").append(map.get(key) + "\n");
                    }

                    copyManager.copyIn("COPY " + ttt.getInputData().getTable() + " FROM STDIN USING DELIMITERS ','", new ByteArrayInputStream(sb.toString().getBytes("UTF-8")));
                    String deleteTime = format.format(new Date(durationEndTime));
                    String sql = deleteSql;
                    sql = sql + "'" + deleteTime + "'";
                    statement.execute(sql);

                    baseConn.commit();
                    baseConn.purgeTimerTasks();

                    log.info("Use copy to successfully write a batch of JSON format data to TimeScaleDB database, the length is:[{}]", map.size());
                    log.info("delete a batch data of {} before", format.format(new Date(durationEndTime)));

                    sb.setLength(0);
                    endTime = System.currentTimeMillis() + ttt.getFrequency() * 1000;
                    durationEndTime = System.currentTimeMillis() - ttt.getDuration() * 3600000;
                }
                consumer.commitAsync();
            }
        } catch (Exception e) {
            log.error("Parsing kafka json format data to write data to postgresql error message is as follows:[{}]", e);
            close();
            init();
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
     * 关闭部分资源
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
            if (dds != null) {
                dds.discardConnection(connection);
            }
        } catch (SQLException e) {
            log.error("The closing resource error message is as follows: [{}]", e.getStackTrace());
        }
    }

    /**
     * 关闭所有资源
     */
    private void closeAll() {
        close();
        if (ttt != null) {
            ttt = null;
        }
        if (consumer != null) {
            consumer.close();
        }
    }
}
