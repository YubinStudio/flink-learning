package com.yu.datastreamapi.transformer;

import com.yu.pojo.Event;
import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

public class TransformFilterTest {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        //从元素中读取数据
        DataStreamSource<Event> fromElements = env.fromElements(
                new Event("Tom", "./cart", 1000L),
                new Event("Judy", "./home", 2000L),
                new Event("Martin", "./home", 4000L)
        );
        //1. 传入自定义类
        SingleOutputStreamOperator<Event> filter1 = fromElements.filter(new MyFilter());

        //2. 匿名内部类
        SingleOutputStreamOperator<Event> filter2 = fromElements.filter(new FilterFunction<Event>() {
            @Override
            public boolean filter(Event value) throws Exception {
                return value.user.equals("Tom");
            }
        });

        //3. 传入Lambda表达式
        SingleOutputStreamOperator<Event> filter3 = fromElements.filter(data -> data.user.equals("Martin"));

        filter1.print("Judy");
        filter2.print("Tom");
        filter3.print("Martin");
        env.execute();
    }

    public static class MyFilter implements FilterFunction<Event> {

        @Override
        public boolean filter(Event value) throws Exception {
            return value.user.equals("Judy");
        }
    }
}
