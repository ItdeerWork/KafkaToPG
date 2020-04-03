package cn.itdeer.common;

import com.alibaba.druid.pool.DruidDataSource;
import com.alibaba.druid.pool.DruidDataSourceFactory;
import com.alibaba.fastjson.JSON;
import lombok.extern.slf4j.Slf4j;

import java.io.*;
import java.sql.Connection;
import java.sql.SQLException;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.ConcurrentHashMap;

/**
 * Description : 解析运行配置文件
 * PackageName : cn.itdeer.kafka.tools
 * ProjectName : Producer
 * CreatorName : itdeer.cn
 * CreateTime : 2019/7/9/15:27
 */
@Slf4j
public class InitConfig {

    private static String configFileName = "runtime.json";
    private static StringBuffer sb = new StringBuffer();
    private static ConfigBean cb;
    private static Map<String, DruidDataSource> map = new ConcurrentHashMap<>();

    /**
     * 静态代码块，加载配置文件
     */
    static {
        String filePath = System.getProperty("user.dir") + File.separator + "config" + File.separator + configFileName;
        try (
                BufferedReader br = new BufferedReader(new InputStreamReader(new FileInputStream(filePath), "UTF-8"))
        ) {
            String line;
            while ((line = br.readLine()) != null) {
                sb.append(line);
            }
            cb = JSON.parseObject(sb.toString(), ConfigBean.class);
            log.info("Reading the configuration file is complete [{}]", configFileName);

            log.info("Initializes connection pool information ......");
            initConnectionList();
        } catch (IOException e) {
            log.error("Error reading configuration file [{}] error message is as follows:[{}]", configFileName, e.getStackTrace());
        }
    }

    /**
     * 返回数据源信息列表
     *
     * @return List<Datasource> 数据源列表
     */
    public static List<Datasource> getDataSource() {
        return cb.getDatasource();
    }

    /**
     * 获取配置的实体对象
     *
     * @return ConfigBean 配置实体对象
     */
    public static ConfigBean getConfigBean() {
        return cb;
    }

    /**
     * 获取连接池队列
     *
     * @return Map<String, Object> 连接池引用队列
     */
    public static Map<String, DruidDataSource> getConnectionMap() {
        return map;
    }

    /**
     * 为表创建表结构
     *
     * @param connection JDBC连接
     * @param table      表名
     * @param mapping    表字段信息
     */
    private static void createTable(Connection connection, String table, String mapping) {
        String sql = splitMapping(mapping, table);
        try {
            connection.prepareStatement(sql).execute();
            log.info("The table structure was created successfully for the table name [{}]", table);
        } catch (SQLException e) {
            log.info("The failure information for creating the table structure for the table name [{}] is as follows :[{}]", table, e.getStackTrace());
        } finally {
            if (connection != null) {
                try {
                    connection.close();
                } catch (SQLException e) {
                    log.info("An exception occurred when the connection was closed. The exception information is as follows: [{}]:", e.getStackTrace());
                }
            }
        }
    }

    /**
     * 拆分映射信息
     *
     * @param mapping 映射信息
     * @param table   表名
     * @return SQL 创建表的SQL
     */
    private static String splitMapping(String mapping, String table) {
        StringBuffer sb = new StringBuffer();
        sb.append("create table if not exists ").append(table).append("(");
        String tmp = mapping + ",";
        String[] splitMapping = tmp.split(",");

        for (String s : splitMapping) {
            String[] field = s.split(":");
            String ss = field[0] + " " + field[1] + ",";
            sb.append(ss);
        }

        String sql = sb.toString();
        sql = sql.substring(0, sql.length() - 1) + ")";
        return sql;
    }

    /**
     * 初始化连接池信息列表
     */
    private static void initConnectionList() {
        List<Datasource> ds_list = getDataSource();
        for (Datasource ds : ds_list) {
            InputData id = ds.getTopicToTable().getInputData();
            DruidDataSource druidDataSource = getConnection(id.getDatabase(), id.getTable());

            try {
                createTable(druidDataSource.getConnection(), id.getTable(), ds.getTopicToTable().getMapping());
            } catch (SQLException e) {
                e.printStackTrace();
                log.info("An exception occurred when creating the table. The exception information is as follows: [{}]:", e.getStackTrace());
            }
            map.put(id.getTable(), druidDataSource);
        }
        log.info("Initialization of connection pool information is complete");
    }

    /**
     * 获取连接池
     *
     * @param database
     * @param table
     * @return
     */
    private static DruidDataSource getConnection(String database, String table) {
        Properties properties = new Properties();

        properties.setProperty("type", "com.alibaba.druid.pool.DruidDataSource");
        properties.setProperty("driverClassName", "org.postgresql.Driver");

        properties.setProperty("url", "jdbc:postgresql://" + cb.getPostgresql().getHost() + ":" + cb.getPostgresql().getPort() + "/" + database + "?characterEncoding=utf-8&useServerPrepStmts=true&rewriteBatchedStatements=true&connectTimeout=600&loginTimeout=600&socketTimeout=600&tcpKeepAlive=true");
        properties.setProperty("username", cb.getPostgresql().getUser());
        properties.setProperty("password", cb.getPostgresql().getPassword());
        properties.setProperty("filters", "stat,wall,log4j");
        properties.setProperty("maxActive", "20");
        properties.setProperty("initialSize", "3");
        properties.setProperty("maxWait", "60000");
        properties.setProperty("minIdle", "3");
        properties.setProperty("timeBetweenEvictionRunsMillis", "600000");
        properties.setProperty("minEvictableIdleTimeMillis", "300000");
        properties.setProperty("testWhileIdle", "true");
        properties.setProperty("testOnBorrow", "true");
        properties.setProperty("testOnReturn", "false");
        properties.setProperty("poolPreparedStatements", "true");
        properties.setProperty("maxOpenPreparedStatements", "20");
        properties.setProperty("asyncInit", "true");
        properties.setProperty("connectionProperties", "druid.stat.mergeSql=true;druid.stat.slowSqlMillis=5000");

        try {
            DruidDataSource druidDataSource = (DruidDataSource) DruidDataSourceFactory.createDataSource(properties);
            log.info("The connection pool was created successfully for the table name [{}]", table);
            return druidDataSource;
        } catch (Exception e) {
            log.error("Error creating data connection pool for [{}] Error message is as follows:[{}]", table, e.getStackTrace());
        }
        return null;
    }

}
