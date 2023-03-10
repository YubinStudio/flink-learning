package com.yu.datastreamapi.transformer;

import com.yu.pojo.Event;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

public class TransformMapTest {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        //从元素中读取数据
        DataStreamSource<Event> fromElements = env.fromElements(
                new Event("Tom", "./cart", 1000L),
                new Event("Judy", "./home", 2000L),
                new Event("Martin", "./home", 4000L)
        );
        // 1 .使用自定义类,实现MapFunction; map提取出user
        SingleOutputStreamOperator<String> userCustom = fromElements.map(new MyMapper());

        // 2. 使用匿名内部类实现MapFunction; map提取出user
        SingleOutputStreamOperator<String> userAnonymous = fromElements.map(new MapFunction<Event, String>() {
            @Override
            public String map(Event value) throws Exception {
                return value.user;
            }
        });
        // 3. 使用 Lambda ; map提取出user
        SingleOutputStreamOperator<String> userLambda = fromElements.map(data -> data.user);

        userCustom.print("1");
        userAnonymous.print("2");
        userLambda.print("3");

        env.execute();
    }

    public static class MyMapper implements MapFunction<Event, String> {

        @Override
        public String map(Event value) throws Exception {
            return value.user;
        }

    }
}
