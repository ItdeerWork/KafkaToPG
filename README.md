### 编译运行环境

```
编译环境：
JDK: 1.8+
Maven：3.3.9+

打包命令：
mvn clean package

运行命令：
在KafkaToPG-2.0.0.jar所在的目录，创建名为config的目录，
在config目录中创建runtime.json的配置文件，配置一下必要的参数即可运行起来

java -jar KafkaToPG-1.0.0.jar

会在Jar包所在的目录创建logs目录，目录里面有相关的运行日志，可供查阅

后台运行启动命令

nohup java -jar KafkaToPG-1.0.0.jar &
```

### 应用程序整体的配置说明

```
{
  "kafka": {                            //kafka的基础配置信息
    "groupIdSuffix": "GROUP_ID",        //消费组的后缀，完整的组别名称为[表名_groupIdSuffix]
    "autoCommitIntervalMs": "1000",     
    "maxPollRecords": "20000",
    "fetchMaxBytes": "10485760",
    "fetchMaxWaitMs": "1000",
    "sessionTimeoutMs": "10000",        //session的超时时间
    "heartbeatIntervalMs": "1000",
    "maxPollIntervalMs": "60000",
    "autoOffsetReset": "earliest",      //Offset消费的位置
    "enableAutoCommit": "true",         //是否自动提交Offset
    "keyDeserializerClass": "org.apache.kafka.common.serialization.StringDeserializer",               //Key的
    "valueDeserializerClass": "org.apache.kafka.common.serialization.StringDeserializer"
  },
  "postgresql": {                       //postgresql的基础配置信息
    "user": "demo",                     //连接的用户
    "password": "12345678",             //连接密码
    "port": "5432",                     //端口
    "host": "192.168.1.220"             //主机IP
  },
  "datasource": [                       //数据流动映射配置
    {
      "topicToTable": {                 //从Topic到数据库表，可以有多个
        "outputData": {                 //数据输出
          "bootstrapServers": "192.168.1.220:9092",     //Kafk的机器连接信息，多个以逗号分隔host1:port1,host2:port2
          "topicName": "demo",          //主题名称
        },
        "inputData": {                  //数据输入
          "database": "demo",           //数据库名称
          "table": "demo01"             //数据表
        },
        "commons": {                    //其他配置
          
          "type" : "batch",             //选用哪一种方式进行插入数据（当选择batch方式，则需要配置batchSize的属性，当配置为copy时，可以不用配置batchSize）
          "batchSize": "2000"           //批处理的大小
        },
        "frequency": "30",              //频率 30秒插入一批最新数据
        "duration": "24",               //保留数据时长 保留最近24小时的数据
        "threads": "2",                 //启动的线程数
        "mapping": "tagName:text,tagValue:decimal,isGood:boolean,sendTS:timestamp,piTS:timestamp"               //对应关系，CSV要按照数据顺序配置
      }
    }
  ]
}

```

### 应用程序整体的配置详细说明

> 以下所有的配置在程序中没有硬编码默认值，所以启动程序必须有以下配置信息，可以更改配置值，不能为空

[1] kafka

 - groupIdSuffix

    ```
    参数名称：groupIdSuffix(消费组后缀)
    默认值：GROUP_ID
    建议：可以更改其他字符串
    ```

 - autoCommitIntervalMs
    
    ```
    参数名称：autoCommitIntervalMs(自动提交间隔)
    默认值：1000
    建议：可以更改为5000
    ```
    
 - maxPollRecords
    
    ```
    参数名称：maxPollRecords(Consumer每次调用poll()时取到的records的最大数)
    默认值：20000
    建议：可以根据实际情况更改
    ```
    
 - fetchMaxBytes
    
    ```
    参数名称：fetchMaxBytes
    默认值：52428800(5M)
    说明：一次fetch请求，从一个broker中取得的records最大大小。
        如果在从topic中第一个非空的partition取消息时，如果取到的第一个record的大小就超过这个配置时，仍然会读取这个record，
        也就是说在这片情况下，只会返回这一条record。
        broker、topic都会对producer发给它的message size做限制。
        所以在配置这值时，可以参考broker的message.max.bytes 和 topic的max.message.bytes的配置。
    建议：可以根据实际情况更改
    ```

 - fetchMaxWaitMs
    
    ```
    参数名称：fetchMaxWaitMs
    默认值：1000
    说明：Fetch请求发给broker后，在broker中可能会被阻塞的（当topic中records的总size小于fetch.min.bytes时），
        此时这个fetch请求耗时就会比较长。这个配置就是来配置consumer最多等待response多久。
    建议：可以根据实际情况更改
    ```
    
 - sessionTimeoutMs
    
    ```
    参数名称：sessionTimeoutMs
    默认值：10000
    说明：Consumer session 过期时间。
        这个值必须设置在broker configuration中的group.min.session.timeout.ms 与 group.max.session.timeout.ms之间。
    建议：可以根据实际情况更改
    ```

 - heartbeatIntervalMs
    
    ```
    参数名称：heartbeatIntervalMs
    默认值：3000
    说明：心跳间隔。心跳是在consumer与coordinator之间进行的。心跳是确定consumer存活，加入或者退出group的有效手段。
           这个值必须设置的小于session.timeout.ms，因为：当Consumer由于某种原因不能发Heartbeat到coordinator时，
           并且时间超过session.timeout.ms时，就会认为该consumer已退出，
           它所订阅的partition会分配到同一group 内的其它的consumer上。
           通常设置的值要低于session.timeout.ms的1/3。
    建议：可以根据实际情况更改
    ```

 - maxPollIntervalMs
    
    ```
    参数名称：maxPollIntervalMs
    默认值：3000
    说明：前面说过要求程序中不间断的调用poll()。如果长时间没有调用poll，
           且间隔超过这个值时，就会认为这个consumer失败了。
    建议：可以根据实际情况更改
    ```

 - autoOffsetReset
    
    ```
    参数名称：autoOffsetReset
    默认值：earliest
    说明：这个配置项，是告诉Kafka Broker在发现kafka在没有初始offset，
        或者当前的offset是一个不存在的值（如果一个record被删除，就肯定不存在了）时，该如何处理。它有4种处理方式：
        1） earliest：自动重置到最早的offset。
        2） latest：看上去重置到最晚的offset。
        3） none：如果边更早的offset也没有的话，就抛出异常给consumer，告诉consumer在整个consumer group中都没有发现有这样的offset。
        4） 如果不是上述3种，只抛出异常给consumer。
    建议：可以根据实际情况更改
    ```

 - enableAutoCommit
    
    ```
    参数名称：enableAutoCommit
    默认值：false
    说明：Consumer 在commit offset时有两种模式：自动提交，手动提交。手动提交在前面已经说过
    建议：这个是根据批量插入勤=情况默认就是不自动提交Offset
    ```

 - keyDeserializerClass,valueDeserializerClass
    
    ```
    参数名称：keyDeserializerClass
    默认值：org.apache.kafka.common.serialization.StringDeserializer，org.apache.kafka.common.serialization.StringDeserializer
    说明：Message record 的key, value的反序列化类。
    建议：根据数据进行修改
    ```

[2] postgresql

 - user
    
    ```
    参数名称：user
    默认值：demo
    说明：根据需要插入数据库的配置进行更改
    ```

 - password
    
    ```
    参数名称：password
    默认值：12345678
    说明：针对以上的用户修改密码
    ```
    
 - port
    
    ```
    参数名称：port
    默认值：5432
    说明：针对使用的PG库的端口进行更改
    ```   
  
 - host
    
    ```
    参数名称：host
    默认值：192.168.1.220
    说明：PG所在机器IP,需要PG能IP连接
    ``` 

[3] datasource[topicToTable]

1. outputData
    
     - bootstrapServers 
    
       ```
       参数名称：bootstrapServers
       默认值：192.168.1.220:9092
       说明：Kafka服务所在机器的地址，多个之间使用","连接
       ``` 

     - topicName 
    
       ```
       参数名称：topicName
       默认值：demo
       说明：需要取的数据主题
       ``` 

2. inputData
    
     - database 
    
       ```
       参数名称：database
       默认值：demo
       说明：PG库名称
       ``` 

     - table 
    
       ```
       参数名称：table
       默认值：demo
       说明：PG的库的表
       ``` 

3. threads 
 
    ```
    参数名称：threads
    默认值：1
    说明：针对获取kafka主题的数据需要并行的线程数
    ``` 

4. duration 
 
    ```
    参数名称：duration
    默认值：24
    说明：保留数据时长 保留最近24小时的数据
    ``` 
5. frequency 
 
    ```
    参数名称：frequency
    默认值：30
    说明：频率 30秒插入一批最新数据
    ``` 
6. mapping

    ```
    参数名称：mapping
    默认值：name:text,age:integer
    说明：Kafka主题数据对应PG表的字段，中间使用":"分割，多个字段之间使用","分割
    ``` 