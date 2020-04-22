package cn.itdeer.utils;

import com.alibaba.druid.pool.DruidDataSource;
import com.alibaba.druid.pool.DruidDataSourceFactory;
import com.alibaba.druid.pool.DruidPooledConnection;
import lombok.extern.slf4j.Slf4j;

import java.sql.SQLException;
import java.util.Properties;

/**
 * Description : 数据库连接池
 * PackageName : cn.itdeer.utils
 * ProjectName : KafkaToPG
 * CreatorName : itdeer.cn
 * CreateTime : 2020/4/21/12:05
 */
@Slf4j
public enum ConnectionPool {
    INSTANCE;

    /**
     * 构造函数
     */
    ConnectionPool() {
        init();
        addShutdownHook();
    }

    private DruidDataSource dataSource = null;

    /**
     * 初始化连接池
     */
    private void init() {
        Properties properties = new DataBaseConfig().getProp();
        try {
            dataSource = (DruidDataSource) DruidDataSourceFactory.createDataSource(properties);
        } catch (Exception e) {
            log.error("An exception occurred to initialize the DataBase library connection pool. The exception information is as follows [{}]", e.getStackTrace());
        }
    }

    /**
     * 注册资源关闭线程
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
        if (dataSource != null && !dataSource.isClosed()) {
            dataSource.close();
        }
    }

    /**
     * 获取数据库连接
     *
     * @return 数据库连接
     * @throws SQLException SQL异常
     */
    public DruidPooledConnection getConnection() throws SQLException {
        if (!dataSource.isKeepAlive()) {
            init();
        }

        DruidPooledConnection dpc;
        try {
            dpc = dataSource.getConnection();
            if (dpc != null)
                return dpc;
        } catch (SQLException e) {
            log.error("Get the connection empty from the connection pool error message: [{}]", e.getStackTrace());
        }
        throw new SQLException();
    }
}