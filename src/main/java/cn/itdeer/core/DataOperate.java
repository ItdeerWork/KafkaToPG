package cn.itdeer.core;

import cn.itdeer.common.Datasource;
import cn.itdeer.utils.ConnectionPool;
import lombok.extern.slf4j.Slf4j;
import org.postgresql.copy.CopyManager;
import org.postgresql.core.BaseConnection;

import java.io.ByteArrayInputStream;
import java.sql.Connection;
import java.sql.SQLException;
import java.sql.Statement;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.Map;

/**
 * Description : 操作数据
 * PackageName : cn.itdeer.core
 * ProjectName : KafkaToPG
 * CreatorName : itdeer.cn
 * CreateTime : 2020/4/23/22:30
 */
@Slf4j
public class DataOperate implements Runnable {

    public static final String DATA_FORMAT = "yyyy-MM-dd HH:mm:ss";
    public static final SimpleDateFormat format = new SimpleDateFormat(DATA_FORMAT);

    private Connection connection;
    private Statement statement;
    private BaseConnection baseConn;
    private CopyManager copyManager;
    private StringBuffer sb;

    private String deleteSql;
    private Datasource ds;
    private Map<String, String> map;

    /**
     * 构造函数
     *
     * @param ds  DataSource 实例
     * @param map 存放最新数据的集合
     */
    public DataOperate(Datasource ds, Map<String, String> map) {
        this.ds = ds;
        this.deleteSql = "delete from " + ds.getTable() + " where loadtime < ";
        this.map = map;

        sb = new StringBuffer();
        initCopyManager();
    }

    /**
     * 初始化连接信息
     *
     * @return CopyManager 通道管理实例
     */
    private CopyManager initCopyManager() {
        try {
            connection = ConnectionPool.INSTANCE.getConnection();
            connection.setAutoCommit(false);
            statement = connection.createStatement();

            baseConn = (BaseConnection) connection.getMetaData().getConnection();
            baseConn.setAutoCommit(false);
            copyManager = new CopyManager(baseConn);
        } catch (Exception e) {
            log.error("Error retrieving connection from connection pool or instantiating processing instance. Error message:[{}]", e.getStackTrace());
        }
        return copyManager;
    }

    /**
     * 覆盖父类方法，新线程运行任务
     */
    @Override
    public void run() {
        insertData();
        deleteData();
    }

    /**
     * 插入数据
     */
    private void insertData() {
        try {
            for (String key : map.keySet()) {
                sb.append(key + ",").append(map.get(key) + "\n");
            }

            copyManager.copyIn("COPY " + ds.getTable() + " FROM STDIN USING DELIMITERS ','", new ByteArrayInputStream(sb.toString().getBytes("UTF-8")));
            log.info("Use copy to successfully write {} pieces of data to table {} of timescaleDB", map.size(), ds.getTable());
            baseConn.commit();
            baseConn.purgeTimerTasks();

            sb.setLength(0);
        } catch (Exception e) {
            log.error("Failed to write data to table {} of timescaleDB using copy The error message is as follows :{}", ds.getTable(), e.getStackTrace());
        }
    }

    /**
     * 删除数据
     */
    private void deleteData() {
        long durationEndTime = System.currentTimeMillis() - ds.getDuration() * 3600000;
        String deleteTime = format.format(new Date(durationEndTime));

        String sql = deleteSql;
        sql = sql + "'" + deleteTime + "'";

        try {
            int delleteNumber = statement.executeUpdate(sql);
            log.info("Delete {} data before {} in table {} successfully", delleteNumber, deleteTime, ds.getTable());
        } catch (SQLException e) {
            log.info("The data before {} in table {} failed to be deleted. The error message is as follows :{}", deleteTime, ds.getTable(), e.getStackTrace());
        }
    }

}
