package com.yu.datastreamapi.process;

import com.yu.pojo.Event;
import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.streaming.api.functions.source.SourceFunction;
import org.apache.flink.util.Collector;

import java.sql.Timestamp;
import java.time.Duration;

public class EventTimeTimer {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        //手动设置事件时间水位线
//        SingleOutputStreamOperator<Event> stream = env.addSource(new ClickSource())
        SingleOutputStreamOperator<Event> stream = env.addSource(new CustomSource())
                .assignTimestampsAndWatermarks(WatermarkStrategy.<Event>forBoundedOutOfOrderness(Duration.ZERO)
                        .withTimestampAssigner(new SerializableTimestampAssigner<Event>() {
                            @Override
                            public long extractTimestamp(Event element, long recordTimestamp) {
                                return element.timestamp;
                            }
                        })
                );

        stream.keyBy(data -> data.user)
                .process(new KeyedProcessFunction<String, Event, String>() {
                    @Override
                    public void processElement(Event value, Context ctx, Collector<String> out) throws Exception {
                        //获取当前时间,即当前数据时间
                        long eventTime = ctx.timestamp();
                        out.collect(ctx.getCurrentKey() + " 数据到达, 时间 : " + new Timestamp(eventTime) + " watermark: " + ctx.timerService().currentWatermark());

                        //注册一个10秒后的事件时间定时器
                        ctx.timerService().registerEventTimeTimer(eventTime + 10 * 1000L);
                    }

                    @Override
                    public void onTimer(long timestamp, OnTimerContext ctx, Collector<String> out) throws Exception {
                        out.collect(ctx.getCurrentKey() + " 定时器触发，触发时间 : " + new Timestamp(timestamp) + " watermark: " + ctx.timerService().currentWatermark());
                    }
                }).print();

        env.execute();
    }

    //测试水位线
    public static class CustomSource implements SourceFunction<Event> {
        @Override
        public void run(SourceContext<Event> ctx) throws Exception {
            ctx.collect(new Event("Mary", "./home", 1000L));
            Thread.sleep(5000L);

            ctx.collect(new Event("Alice", "./home", 11000L));
            Thread.sleep(5000L);

            ctx.collect(new Event("Bob", "./home", 11001L));
            Thread.sleep(5000L);

        }

        @Override
        public void cancel() {

        }
    }
}
