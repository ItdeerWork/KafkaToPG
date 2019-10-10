package cn.itdeer;

import cn.itdeer.common.Constants;
import cn.itdeer.common.Datasource;
import cn.itdeer.common.InitConfig;
import cn.itdeer.core.InitConsumer;
import lombok.extern.slf4j.Slf4j;

import java.util.List;

/**
 * Description : 程序的入口
 * PackageName : cn.itdeer
 * ProjectName : KafkaToPG
 * CreatorName : itdeer.cn
 * CreateTime : 2019/8/13/15:17
 */
@Slf4j
public class Main {
    public static void main(String[] args) {
        List<Datasource> list = InitConfig.getDataSource();
        for (Datasource ds : list) {
            String[] fields = splitMapping(ds.getTopicToTable().getMapping());
            for (int i = 1; i <= ds.getTopicToTable().getCommons().getThreads(); i++) {
                new InitConsumer(InitConfig.getConfigBean().getKafka(), ds.getTopicToTable(), fields);
            }
        }
    }

    /**
     * 拆分数据结构映射
     *
     * @param mapping 结构映射
     * @return 字段数组
     */
    private static String[] splitMapping(String mapping) {
        String tmp = mapping + ",";
        String[] splitMapping = tmp.split(Constants.COMMA);
        String[] field = new String[splitMapping.length];
        for (int i = 0; i < splitMapping.length; i++) {
            field[i] = splitMapping[i].split(Constants.COLON)[0];
        }
        return field;
    }
}
