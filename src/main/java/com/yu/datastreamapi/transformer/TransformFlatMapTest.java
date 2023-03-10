package com.yu.datastreamapi.transformer;

import com.yu.pojo.Event;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.typeinfo.TypeHint;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.util.Collector;

public class TransformFlatMapTest {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        //从元素中读取数据
        DataStreamSource<Event> fromElements = env.fromElements(
                new Event("Tom", "./cart", 10000L),
                new Event("Judy", "./home", 20000L),
                new Event("Martin", "./home", 4000L)
        );
        //1. 传入自定义类
        SingleOutputStreamOperator<String> flatMap1 = fromElements.flatMap(new MyFlatMap());

        //2. 匿名内部类
        SingleOutputStreamOperator<String> flatMap2 = fromElements.flatMap(new FlatMapFunction<Event, String>() {

            @Override
            public void flatMap(Event value, Collector<String> out) throws Exception {
                out.collect(value.user + " -- " + value.url + " -- " + value.timestamp.toString());
            }
        });

        //3. 传入Lambda表达式
        SingleOutputStreamOperator<String> flatMap3 = fromElements.flatMap((Event value, Collector<String> out) -> {
            if (value.user.equals("Tom"))
                out.collect(value.url);
            out.collect(value.timestamp.toString());
        }).returns(new TypeHint<String>() {});


//        flatMap1.print("flatMap1");
        flatMap2.print("flatMap2");
//        flatMap3.print("flatMap3");
        env.execute();
    }

    public static class MyFlatMap implements FlatMapFunction<Event, String> {

        @Override
        public void flatMap(Event value, Collector<String> out) throws Exception {
            out.collect(value.user);
            out.collect(value.url);
            out.collect(value.timestamp.toString());
        }
    }
}
