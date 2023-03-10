package com.yu.datastreamapi.state;

import com.yu.pojo.Action;
import com.yu.pojo.Pattern;
import org.apache.flink.api.common.state.*;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.contrib.streaming.state.EmbeddedRocksDBStateBackend;
import org.apache.flink.runtime.state.hashmap.HashMapStateBackend;
import org.apache.flink.streaming.api.datastream.BroadcastStream;
import org.apache.flink.streaming.api.datastream.DataStreamSink;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.co.KeyedBroadcastProcessFunction;
import org.apache.flink.util.Collector;

/**
 * 用户行为模式检测
 */
public class BroadCast_BehaviorPatternDetect {
    // 定义广播状态描述器
    private final static MapStateDescriptor<Void, Pattern> descriptor = new MapStateDescriptor<>("pattern", Types.VOID, Types.POJO(Pattern.class));

    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        // 配置检查点 10s做一次存储
        env.enableCheckpointing(10000L);
        // 配置哈希表状态后端（HashMapStateBackend）
        env.setStateBackend(new HashMapStateBackend());
        // 配置RocksDB 状态后端（EmbeddedRocksDBStateBackend）
        env.setStateBackend(new EmbeddedRocksDBStateBackend());

        // 用户的行为数据流
        DataStreamSource<Action> actionStream = env.fromElements(
                new Action("Alice", "login"),
                new Action("Alice", "pay"),
                new Action("Bob", "login"),
                new Action("Bob", "order")
        );

        // 行为模式流，基于它构建广播流
        DataStreamSource<Pattern> patternStream = env.fromElements(
                new Pattern("login", "pay"),
                new Pattern("login", "order")
        );


        // 广播行为模式流
        BroadcastStream<Pattern> broadcastStream = patternStream.broadcast(descriptor);

        // 连接两条流
        DataStreamSink<Tuple2<String, Pattern>> matches = actionStream.keyBy(data -> data.userId)
                .connect(broadcastStream)
                .process(new PatternDetector())
                .print();

        env.execute();
    }

    // 实现自定义 KeyedBroadcastProcessFunction
    public static class PatternDetector extends KeyedBroadcastProcessFunction<String, Action, Pattern, Tuple2<String, Pattern>> {
        // 定义状态，保存上一次用户的行为
        ValueState<String> preActionState;

        @Override
        public void open(Configuration parameters) throws Exception {
            preActionState = getRuntimeContext().getState(
                    new ValueStateDescriptor<String>("last-action", String.class)
            );
        }

        @Override
        public void processElement(Action value, ReadOnlyContext ctx, Collector<Tuple2<String, Pattern>> out) throws Exception {
            // 从广播状态获取匹配模式
            ReadOnlyBroadcastState<Void, Pattern> broadcastState = ctx.getBroadcastState(descriptor);
            Pattern pattern = broadcastState.get(null);

            // 获取用户上一次行为
            String preAction = preActionState.value();

            // 判断是否匹配
            if (pattern != null && preAction != null) {
                if (pattern.action1.equals(preAction) && pattern.action2.equals(value.action)) {
                    out.collect(new Tuple2<>(ctx.getCurrentKey(), pattern));
                }
            }

            // 没有匹配到更新状态
            preActionState.update(value.action);
        }

        @Override
        public void processBroadcastElement(Pattern value, Context ctx, Collector<Tuple2<String, Pattern>> out) throws Exception {
            // 从上下文中 获取广播状态，并用当前数据更新
            BroadcastState<Void, Pattern> patternState = ctx.getBroadcastState(descriptor);

            patternState.put(null, value);
        }
    }
}
