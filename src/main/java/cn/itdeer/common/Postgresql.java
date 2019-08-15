package cn.itdeer.common;

import lombok.Data;

/**
 * Description : Postgresql的通用配置
 * PackageName : cn.itdeer.common
 * ProjectName : KafkaToPG
 * CreatorName : itdeer.cn
 * CreateTime : 2019/8/13/23:03
 */

@Data
public class Postgresql {
    private String user;
    private String password;
    private String port;
    private String host;
}