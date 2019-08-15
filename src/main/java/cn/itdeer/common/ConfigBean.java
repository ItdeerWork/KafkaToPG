
package cn.itdeer.common;

import lombok.Data;

import java.util.List;

/**
 * Description : 应用配置实体
 * PackageName : cn.itdeer.common
 * ProjectName : KafkaToPG
 * CreatorName : itdeer.cn
 * CreateTime : 2019/8/13/23:03
 */

@Data
public class ConfigBean {
    private Kafka kafka;
    private Postgresql postgresql;
    private List<Datasource> datasource;
}