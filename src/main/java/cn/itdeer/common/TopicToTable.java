package cn.itdeer.common;

import lombok.Data;

/**
 * Description : 数据流向配置
 * PackageName : cn.itdeer.common
 * ProjectName : KafkaToPG
 * CreatorName : itdeer.cn
 * CreateTime : 2019/8/13/23:03
 */

@Data
public class TopicToTable {
    private OutputData outputData;
    private InputData inputData;
    private Integer frequency;
    private Integer duration;
    private Integer threads;
    private String mapping;
}
