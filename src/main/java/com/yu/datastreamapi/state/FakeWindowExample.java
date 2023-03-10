package com.yu.datastreamapi.state;

import com.yu.datastreamapi.datasource.custom.ClickSource;
import com.yu.pojo.Event;
import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.state.MapState;
import org.apache.flink.api.common.state.MapStateDescriptor;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.util.Collector;

import java.sql.Timestamp;
import java.time.Duration;

/**
 * 自定义实现滚动窗口 没10秒计算url访问量
 */
public class FakeWindowExample {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        SingleOutputStreamOperator<Event> stream = env.addSource(new ClickSource())
                .assignTimestampsAndWatermarks(WatermarkStrategy.<Event>forBoundedOutOfOrderness(Duration.ZERO)
                        .withTimestampAssigner(new SerializableTimestampAssigner<Event>() {
                            @Override
                            public long extractTimestamp(Event element, long recordTimestamp) {
                                return element.timestamp;
                            }
                        })
                );

        stream.print("input");

        stream.keyBy(data -> data.url)
                .process(new FakeWindowResult(10000L))
                .print();

        env.execute();
    }

    // 实现自定义KeyedProcessFunction

    public static class FakeWindowResult extends KeyedProcessFunction<String, Event, String> {
        // （窗口大小）
        private Long windowSize;

        public FakeWindowResult(Long windowSize) {
            this.windowSize = windowSize;
        }

        //定义一个MapState 用来保存每个窗口统计的count值
        MapState<Long, Long> windowUrlCountState;

        @Override
        public void open(Configuration parameters) throws Exception {
            windowUrlCountState = getRuntimeContext().getMapState(
                    new MapStateDescriptor<Long, Long>("window-count", Long.class, Long.class)
            );
        }

        @Override
        public void processElement(Event value, Context ctx, Collector<String> out) throws Exception {
            // 没来一条数据 ， 根据时间戳判断属于哪个窗口 （窗口分配器）
            Long windowStart = value.timestamp / windowSize * windowSize;
            Long windowEnd = windowStart + windowSize;

            // 注册end - 1 的定时器
            ctx.timerService().registerEventTimeTimer(windowEnd - 1);

            // 更新状态，进行增量聚合
            if (windowUrlCountState.contains(windowStart)) {
                Long count = windowUrlCountState.get(windowStart);
                windowUrlCountState.put(windowStart, count + 1L);
            } else
                windowUrlCountState.put(windowStart, 1L);
        }

        //定时器触发时 输出计算结果

        @Override
        public void onTimer(long timestamp, OnTimerContext ctx, Collector<String> out) throws Exception {
            Long windowEnd = timestamp + 1;
            Long windowStart = windowEnd - windowSize;
            Long count = windowUrlCountState.get(windowStart);

            out.collect("窗口 " + new Timestamp(windowStart) + " ~ " + new Timestamp(windowEnd)
                    + " url: " + ctx.getCurrentKey()
                    + " count: " + count
            );

            // 模拟窗口的关闭 ，清除map中对的key-value
            windowUrlCountState.remove(windowStart);
        }
    }

}
