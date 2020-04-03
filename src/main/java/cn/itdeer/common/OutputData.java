package cn.itdeer.common;

import lombok.Data;

/**
 * Description : 输出数据源
 * PackageName : cn.itdeer.common
 * ProjectName : KafkaToPG
 * CreatorName : itdeer.cn
 * CreateTime : 2019/8/13/23:03
 */

@Data
public class OutputData {
    private String bootstrapServers;
    private String topicName;
}