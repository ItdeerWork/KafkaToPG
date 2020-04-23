package cn.itdeer.common;

import cn.itdeer.utils.ConnectionPool;
import com.alibaba.fastjson.JSON;
import lombok.extern.slf4j.Slf4j;

import java.io.*;
import java.sql.Connection;
import java.util.List;

import static cn.itdeer.utils.Constants.CONFIG_FILE_DIRECTORY;
import static cn.itdeer.utils.Constants.CONFIG_FILE_NAME;

/**
 * Description : 解析运行配置文件
 * PackageName : cn.itdeer.kafka.tools
 * ProjectName : Producer
 * CreatorName : itdeer.cn
 * CreateTime : 2019/7/9/15:27
 */
@Slf4j
public class InitConfig {

    private static ConfigBean cb;
    private static StringBuffer sb = new StringBuffer();

    /**
     * 静态代码块，加载配置文件
     */
    static {
        String filePath = System.getProperty("user.dir") + File.separator + CONFIG_FILE_DIRECTORY + File.separator + CONFIG_FILE_NAME;
        try (BufferedReader br = new BufferedReader(new InputStreamReader(new FileInputStream(filePath), "UTF-8"))) {
            String line;
            while ((line = br.readLine()) != null) {
                sb.append(line);
            }
            cb = JSON.parseObject(sb.toString(), ConfigBean.class);
            log.info("Reading the configuration file is complete [{}]", CONFIG_FILE_NAME);


            initTable();
        } catch (IOException e) {
            log.error("Error reading configuration file [{}] error message is as follows:[{}]", CONFIG_FILE_NAME, e.getStackTrace());
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
     * 初始化连接池信息列表
     */
    private static void initTable() {
        log.info("Initializes table information ......");
        List<Datasource> list = getDataSource();

        Connection connection;
        try {
            connection = ConnectionPool.INSTANCE.getConnection();
            for (Datasource ds : list) {
                String sql = splitMapping(ds.getMapping(), ds.getTable());
                connection.prepareStatement(sql).execute();
                //connection.prepareStatement("SELECT create_hypertable('" + ds.getTable() + "', 'loadtime');").execute();
                log.info("The table structure was created successfully for the table name [{}]", ds.getTable());
            }
        } catch (Exception e) {
            log.info("An exception occurred when creating the table. The exception information is as follows: [{}]:", e.getStackTrace());
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

}
