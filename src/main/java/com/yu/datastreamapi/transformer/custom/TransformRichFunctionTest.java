package com.yu.datastreamapi.transformer.custom;

import com.yu.pojo.Event;
import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

public class TransformRichFunctionTest {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        //从元素中读取数据
        DataStreamSource<Event> fromElements = env.fromElements(
                new Event("Tom", "./cart", 1000L),
                new Event("Tom", "./home", 20000L),
                new Event("Judy", "./home", 3000L),
                new Event("Judy", "./home", 4000L),
                new Event("Martin", "./home", 5000L),
                new Event("Martin", "./cart", 6000L),
                new Event("Martin", "./prod", 7000L)
        );

        fromElements.map(new MyRichMapper()).setParallelism(2).print();

        env.execute();
    }

    // 实现一个自定义的富函数类
    public static class MyRichMapper extends RichMapFunction<Event, Integer> {
        @Override
        public void open(Configuration parameters) throws Exception {
            super.open(parameters);
            System.out.println("open生命周期" + getRuntimeContext().getIndexOfThisSubtask()  + "号任务被调用");
        }

        @Override
        public void close() throws Exception {
            super.close();
            System.out.println("close生命周期" + getRuntimeContext().getIndexOfThisSubtask()  + "号任务被调用");
        }

        @Override
        public Integer map(Event value) throws Exception {
            return value.user.length();
        }
    }
}
