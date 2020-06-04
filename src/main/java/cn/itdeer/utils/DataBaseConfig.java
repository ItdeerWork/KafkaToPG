package cn.itdeer.utils;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.io.File;
import java.io.FileInputStream;
import java.util.Properties;

import static cn.itdeer.utils.Constants.CONFIG_FILE_DIRECTORY;
import static cn.itdeer.utils.Constants.PG_FILE_NAME;

/**
 * Description : 数据库配置工具类
 * PackageName : cn.itdeer.utils
 * ProjectName : KafkaToPG
 * CreatorName : itdeer.cn
 * CreateTime : 2020/4/21/12:06
 */

public class DataBaseConfig {

    private static final Logger log = LogManager.getLogger(DataBaseConfig.class);

    private static Properties prop = null;
    private static FileInputStream in = null;

    /**
     * 初始化配置信息
     */
    private void init() {
        try {
            if (prop == null) {
                prop = new Properties();
                String runTimePath = System.getProperty("user.dir");
                in = new FileInputStream(new File(runTimePath + File.separator + CONFIG_FILE_DIRECTORY + File.separator + PG_FILE_NAME));
                prop.load(in);
                log.info("loading {} file finish", PG_FILE_NAME);
            }
        } catch (Exception e) {
            log.error("loading {} file appear exception: {}", PG_FILE_NAME, e.getStackTrace());
        } finally {
            if (in != null) {
                try {
                    in.close();
                } catch (Exception e) {
                    log.error("close InputStream appear exception: {}", e.getStackTrace());
                }
            }
        }
    }

    /**
     * 获取DataBase配置属性实例
     *
     * @return Properties 实例
     */
    public Properties getProp() {
        if (prop == null) {
            init();
        }
        return prop;
    }
}
