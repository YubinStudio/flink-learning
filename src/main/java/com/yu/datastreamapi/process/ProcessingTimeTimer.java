package com.yu.datastreamapi.process;

import com.yu.datastreamapi.datasource.custom.ClickSource;
import com.yu.pojo.Event;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.util.Collector;

import java.sql.Timestamp;

public class ProcessingTimeTimer {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        //使用处理时间，不需要设置事件时间水位线
        SingleOutputStreamOperator<Event> stream = env.addSource(new ClickSource());

        stream.keyBy(data -> data.user)
                .process(new KeyedProcessFunction<String, Event, String>() {
                    @Override
                    public void processElement(Event value, Context ctx, Collector<String> out) throws Exception {
                        //获取当前的处理时间
                        long processingTime = ctx.timerService().currentProcessingTime();
                        out.collect(ctx.getCurrentKey() + " 数据到达, 到达时间 : " + new Timestamp(processingTime));

                        //注册一个10秒后的定时器
                        ctx.timerService().registerProcessingTimeTimer(processingTime + 10 * 1000L);
                    }

                    @Override
                    public void onTimer(long timestamp, OnTimerContext ctx, Collector<String> out) throws Exception {
                        out.collect(ctx.getCurrentKey() + "定时器触发，触发时间 : " + new Timestamp(timestamp));
                    }
                }).print();

        env.execute();
    }
}
