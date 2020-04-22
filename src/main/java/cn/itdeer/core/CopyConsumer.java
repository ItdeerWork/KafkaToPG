package cn.itdeer.core;

import cn.itdeer.common.Datasource;
import cn.itdeer.utils.ConnectionPool;
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
    private Datasource ds;

    private BaseConnection baseConn;
    private CopyManager copyManager = null;
    private StringBuffer sb;
    private Connection connection;

    private Map<String, String> map;

    private String deleteSql;
    private Statement statement;

    /**
     * 构造函数 父类进行实例化，这里直接可以使用
     *
     * @param consumer 消费者实例
     * @param ds       数据流向配置实例
     */
    public CopyConsumer(KafkaConsumer<String, String> consumer, Datasource ds) {
        this.consumer = consumer;
        this.ds = ds;

        map = new HashMap<>(500);
        deleteSql = "delete from " + ds.getTable() + " where loadtime < ";

        initCopyManager();
    }

    /**
     * 初始化连接信息
     *
     * @return CopyManager 通道管理实例
     */
    private CopyManager initCopyManager() {
        if (copyManager == null) {
            try {
                sb = new StringBuffer();
                connection = ConnectionPool.INSTANCE.getConnection();
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

        long endTime = System.currentTimeMillis() + ds.getFrequency() * 1000;
        long durationEndTime = System.currentTimeMillis() - ds.getDuration() * 3600000;

        try {
            while (true) {
                ConsumerRecords<String, String> records = consumer.poll(100);
                long nowTime = System.currentTimeMillis();

                try {
                    for (ConsumerRecord<String, String> record : records) {
                        JSONObject data = JSON.parseObject(record.value());
                        map.put(data.getString("tagName"), data.get("tagValue") + "," + data.get("piTS") + "," + data.get("sendTS"));
                    }
                } catch (Exception e) {
                    log.error("An error occurred while parsing json data. The error information is as follows:[{}]", e.getStackTrace());
                }

                if (nowTime >= endTime) {

                    if (connection.isClosed())
                        initCopyManager();

                    for (String key : map.keySet()) {
                        sb.append(key + ",").append(map.get(key) + "\n");
                    }

                    copyManager.copyIn("COPY " + ds.getTable() + " FROM STDIN USING DELIMITERS ','", new ByteArrayInputStream(sb.toString().getBytes("UTF-8")));
                    log.info("Use copy to successfully write a batch of JSON format data to TimeScaleDB database, the length is:[{}]", map.size());

                    String deleteTime = format.format(new Date(durationEndTime));
                    String sql = deleteSql;
                    sql = sql + "'" + deleteTime + "'";
                    int delleteNumber = statement.executeUpdate(sql);
                    log.info("delete {} data before {}",delleteNumber, format.format(new Date(durationEndTime)));

                    baseConn.commit();
                    baseConn.purgeTimerTasks();
                    connection.commit();

                    sb.setLength(0);
                    endTime = System.currentTimeMillis() + ds.getFrequency() * 1000;
                    durationEndTime = System.currentTimeMillis() - ds.getDuration() * 3600000;
                }

                consumer.commitAsync();
            }
        } catch (Exception e) {
            log.error("Parsing kafka json format data to write data to postgresql error message is as follows:[{}]", e);
            close();
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
        } catch (SQLException e) {
            log.error("The closing resource error message is as follows: [{}]", e.getStackTrace());
        }
    }

    /**
     * 关闭所有资源
     */
    private void closeAll() {
        close();
        if (ds != null) {
            ds = null;
        }
        if (consumer != null) {
            consumer.close();
        }
    }
}
