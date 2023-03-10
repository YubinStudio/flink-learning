package com.yu.datastreamapi.datasource;

import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer;

import java.util.Properties;

public class Source4Kafka {
    public static void main(String[] args) throws Exception {
        // 创建环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        // Kafak配置项
        Properties properties = new Properties();
        properties.setProperty("bootstrap.servers", "hadoop01:9092");
        properties.setProperty("group.id", "consumer-group");
        properties.setProperty("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        properties.setProperty("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        properties.setProperty("auto.offset.reset", "latest");

        /**
         * 添加数据源
         * 在hadoop01-kafka目录启动如下命令：
         *  启动kafka自带zookeeper
         *      ./bin/zookeeper-server-start.sh -daemon ./config/zookeeper.properties
         *  启动kafka服务
         *      ./bin/kafka-server-start.sh -daemon ./config/server.properties
         *  启动生产者
         *      ./bin/kafka-console-producer.sh --broker-list hadoop01:9092 --topic clicks
         */
        //添加kafka数据源
        DataStreamSource<String> kafkaStream = env.addSource(new FlinkKafkaConsumer<String>("clicks", new SimpleStringSchema(), properties));
        kafkaStream.print("kafka");

        env.execute();
    }
}
