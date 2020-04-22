package cn.itdeer.common;

import lombok.Data;

/**
 * Description : 数据源信息
 * PackageName : cn.itdeer.common
 * ProjectName : KafkaToPG
 * CreatorName : itdeer.cn
 * CreateTime : 2019/8/13/23:03
 */

@Data
public class Datasource {
    private String topicName;
    private String table;
    private Integer frequency;
    private Integer duration;
    private Integer threads;
    private String mapping;
}