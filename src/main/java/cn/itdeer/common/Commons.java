package cn.itdeer.common;

import lombok.Data;

/**
 * Description : 数据流向通用配置
 * PackageName : cn.itdeer.common
 * ProjectName : KafkaToPG
 * CreatorName : itdeer.cn
 * CreateTime : 2019/8/13/23:03
 */

@Data
public class Commons {
    private Integer threads;
    private Integer batchSize;
}