package cn.itdeer;

import cn.itdeer.common.Datasource;
import cn.itdeer.common.InitConfig;
import cn.itdeer.core.DataOperate;
import cn.itdeer.core.InitConsumer;
import lombok.extern.slf4j.Slf4j;

import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

/**
 * Description : 程序的入口
 * PackageName : cn.itdeer
 * ProjectName : KafkaToPG
 * CreatorName : itdeer.cn
 * CreateTime : 2019/8/13/15:17
 */
@Slf4j
public class Main {

    public static void main(String[] args) throws Exception {
        List<Datasource> list = InitConfig.getDataSource();

        //初始化一个线程池
        ScheduledThreadPoolExecutor executor = (ScheduledThreadPoolExecutor) Executors.newScheduledThreadPool(10);

        for (Datasource ds : list) {
            Map<String, String> map = new ConcurrentHashMap(1000);
            //执行一个周期定时任务
            executor.scheduleAtFixedRate(new DataOperate(ds, map), 0, ds.getFrequency(), TimeUnit.SECONDS);

            for (int i = 1; i <= ds.getThreads(); i++) {
                new InitConsumer(InitConfig.getConfigBean().getKafka(), ds.getTopicName(), map);
            }
            log.info("The tasks for table {} are started and completed", ds.getTable());
            Thread.sleep(5000);
        }
    }

}
