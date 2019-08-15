package cn.itdeer.common;

import lombok.Data;

/**
 * Description : Kafka的通用配置
 * PackageName : cn.itdeer.common
 * ProjectName : KafkaToPG
 * CreatorName : itdeer.cn
 * CreateTime : 2019/8/13/23:03
 */

@Data
public class Kafka {
    private String groupIdSuffix;
    private String autoCommitIntervalMs;
    private String maxPollRecords;
    private String fetchMaxBytes;
    private String fetchMaxWaitMs;
    private String sessionTimeoutMs;
    private String heartbeatIntervalMs;
    private String maxPollIntervalMs;
    private String autoOffsetReset;
    private String enableAutoCommit;
    private String keyDeserializerClass;
    private String valueDeserializerClass;
}