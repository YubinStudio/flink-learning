package com.yu.datastreamapi.state;

import com.yu.datastreamapi.datasource.custom.ClickSource;
import com.yu.pojo.Event;
import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.AggregateFunction;
import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.api.common.functions.RichFlatMapFunction;
import org.apache.flink.api.common.state.*;
import org.apache.flink.api.common.time.Time;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.util.Collector;

import java.time.Duration;

public class StateTest {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        //手动设置事件时间水位线
        SingleOutputStreamOperator<Event> stream = env.addSource(new ClickSource())
                .assignTimestampsAndWatermarks(WatermarkStrategy.<Event>forBoundedOutOfOrderness(Duration.ZERO)
                        .withTimestampAssigner(new SerializableTimestampAssigner<Event>() {
                            @Override
                            public long extractTimestamp(Event element, long recordTimestamp) {
                                return element.timestamp;
                            }
                        })
                );

        stream.keyBy(data -> data.user)
                .flatMap(new MyFlatMap())
                .print();


        env.execute();
    }

    // 自定义继承富函数RichFlatMapFunction,测试 Keyed State
    public static class MyFlatMap extends RichFlatMapFunction<Event, String> {
        // 定义状态
        private ValueState<Event> valueState;
        private ListState<Event> listState;
        private MapState<String, Long> mapState;
        private ReducingState<Event> reducingState;
        private AggregatingState<Event, String> aggregatingState;

        Long count = 0L;

        // 在open方法中赋值
        @Override
        public void open(Configuration parameters) throws Exception {
            ValueStateDescriptor<Event> valueStateDescriptor = new ValueStateDescriptor<>("value-state", Event.class);
            valueState = getRuntimeContext().getState(valueStateDescriptor);

            listState = getRuntimeContext().getListState(new ListStateDescriptor<Event>("list-state", Event.class));
            mapState = getRuntimeContext().getMapState(new MapStateDescriptor<String, Long>("map-state", String.class, Long.class));
            reducingState = getRuntimeContext().getReducingState(new ReducingStateDescriptor<Event>("reduce-state", new ReduceFunction<Event>() {
                @Override
                public Event reduce(Event value1, Event value2) throws Exception {
                    return new Event(value1.user, value1.url, value2.timestamp);
                }
            }, Event.class));
            aggregatingState = getRuntimeContext()
                    .getAggregatingState(
                            new AggregatingStateDescriptor<Event, Long, String>(
                                    "agg-state",
                                    new AggregateFunction<Event, Long, String>() {
                                        @Override
                                        public Long createAccumulator() {
                                            return 0L;
                                        }

                                        @Override
                                        public Long add(Event value, Long accumulator) {
                                            return accumulator + 1L;
                                        }

                                        @Override
                                        public String getResult(Long accumulator) {
                                            return "count: " + accumulator;
                                        }

                                        @Override
                                        public Long merge(Long a, Long b) {
                                            return null;
                                        }
                                    },
                                    Long.class
                            )
                    );

            // TODO 配置状态的TTL（状态生存时间）  设置处理时间(系统时间)1小时清理失效状态
            StateTtlConfig ttlConfig = StateTtlConfig.newBuilder(Time.hours(1))
                    // 设置状态失效更新方式(读写都更新失效状态)
                    .setUpdateType(StateTtlConfig.UpdateType.OnReadAndWrite)
                    // 设置状态的可见性,
                    .setStateVisibility(StateTtlConfig.StateVisibility.ReturnExpiredIfNotCleanedUp)
                    .build();
            // TODO 通过描述器添加TTL配置
            valueStateDescriptor.enableTimeToLive(ttlConfig);
        }

        @Override
        public void flatMap(Event value, Collector<String> out) throws Exception {
            //访问和更新state
            System.out.println("value-origin: "+valueState.value());
            valueState.update(value);
//            System.out.println("value-update: " + valueState.value());

            listState.add(value);
            System.out.println("list-value: " + listState.get());

            mapState.put(value.user, mapState.get(value.user) == null ? 999 : mapState.get(value.user) + 1);

            System.out.println("map-value: " + mapState.get(value.user));

            aggregatingState.add(value);
            System.out.println("agg-value: " + aggregatingState.get());

            reducingState.add(value);
            System.out.println("reduce-value: " + reducingState.get());

            count++;
            System.out.println("count-value: " + count);
            System.out.println("-------------------------------------------------------------");
        }
    }
}
