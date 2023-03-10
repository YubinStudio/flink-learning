package com.yu.datastreamapi.sink;

import com.yu.pojo.Event;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaProducer;

import java.util.Properties;

public class SinkToKafkaTest {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        //从Kafka中读取数据
        // Kafak配置项
        Properties properties = new Properties();
        properties.setProperty("bootstrap.servers", "hadoop01:9092");
        properties.setProperty("group.id", "consumer-group");
        properties.setProperty("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        properties.setProperty("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        properties.setProperty("auto.offset.reset", "latest");

        //添加kafka数据源
        DataStreamSource<String> kafkaStream = env.addSource(new FlinkKafkaConsumer<String>("clicks", new SimpleStringSchema(), properties));

        //用flink将string转换成event对象
        SingleOutputStreamOperator<String> result = kafkaStream.map(new MapFunction<String, String>() {
            @Override
            public String map(String value) throws Exception {
                String[] fields = value.split(",");
                return new Event(fields[0].trim(), fields[1].trim(), Long.valueOf(fields[2].trim())).toString();
            }
        });
        /**
         * 添加数据源
         * 在hadoop01-kafka目录启动如下命令：
         *  启动kafka自带zookeeper
         *      ./bin/zookeeper-server-start.sh -daemon ./config/zookeeper.properties
         *  启动kafka服务
         *      ./bin/kafka-server-start.sh -daemon ./config/server.properties
         *  启动生产者
         *      ./bin/kafka-console-producer.sh --broker-list localhost:9092 --topic clicks
         *  启动消费者 :
         *      ./bin/kafka-console-consumer.sh --bootstrap-server localhost:9092 --topic events
         */

        result.addSink(new FlinkKafkaProducer<String>("hadoop01:9092", "events", new SimpleStringSchema()));

        env.execute();
    }
}
