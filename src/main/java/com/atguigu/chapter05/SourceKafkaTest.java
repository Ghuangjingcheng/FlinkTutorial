package com.atguigu.chapter05;

import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer;

import java.util.Properties;

/**
 * @author:hjc
 * @create 2022/8/26 17:47:08
 */

public class SourceKafkaTest {
    public static void main(String[] args) throws Exception {
        // 1.创建环境，设置并行度
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        // 2.创建配置对象
        Properties properties = new Properties();
        properties.setProperty("bootstrap.servers", "hadoop102:9092");// 配置启动服务主机和端口
        properties.setProperty("group.id", "consumer-group");// 配置组id
        properties.setProperty("key.deserializer",
                "org.apache.kafka.common.serialization.StringDeserializer"); //配置读取key类型
        properties.setProperty("value.deserializer",
                "org.apache.kafka.common.serialization.StringDeserializer"); //配置读取value类型
        properties.setProperty("auto.offset.reset", "latest");
        /**
         * earliest
         * 当各分区下有已提交的offset时，从提交的offset开始消费；无提交的offset时，从头开始消费
         * latest
         * 当各分区下有已提交的offset时，从提交的offset开始消费；无提交的offset时，消费新产生的该分区下的数据
         */

        // param1：主题， param2：读取数据类型， param3：配置
        DataStreamSource<String> stream = env.addSource(new FlinkKafkaConsumer<String>(
                "clicks",
                new SimpleStringSchema(),
                properties
        ));

        stream.print("Kafka");

        env.execute();
    }
}
