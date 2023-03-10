package com.yu.datastreamapi.transformer;

import com.yu.pojo.Event;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.streaming.api.datastream.DataStreamSink;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

public class TransformSimpleAggTest {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        //从元素中读取数据
        DataStreamSource<Event> fromElements = env.fromElements(
                new Event("Tom", "./cart", 1000L),
                new Event("Tom", "./home", 20000L),
                new Event("Judy", "./home", 3000L),
                new Event("Judy", "./home", 4000L),
                new Event("Martin", "./home",  5000L),
                new Event("Martin", "./cart", 6000L),
                new Event("Martin", "./prod", 7000L)
        );

        // 获取用户最新的操作
        // 1 使用匿名内部类
        DataStreamSink<Event> maxStream = fromElements.keyBy(new KeySelector<Event, String>() {
            @Override
            public String getKey(Event value) throws Exception {
                return value.user;
            }
        }).max("timestamp")
          .print("max");

        // 2 使用LambDa表达式
        fromElements.keyBy(data -> data.user)
                .maxBy("timestamp")
                .print("maxBy: ");

        env.execute();
    }
}
