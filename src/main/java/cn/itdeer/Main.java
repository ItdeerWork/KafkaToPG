package cn.itdeer;

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
            for (int i = 1; i <= ds.getThreads(); i++) {
                new InitConsumer(InitConfig.getConfigBean().getKafka(), ds);
            }
        }
    }

}
