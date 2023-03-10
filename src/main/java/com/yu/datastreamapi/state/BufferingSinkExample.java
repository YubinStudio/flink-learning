package com.yu.datastreamapi.state;

import com.yu.datastreamapi.datasource.custom.ClickSource;
import com.yu.pojo.Event;
import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.state.ListState;
import org.apache.flink.api.common.state.ListStateDescriptor;
import org.apache.flink.runtime.state.FunctionInitializationContext;
import org.apache.flink.runtime.state.FunctionSnapshotContext;
import org.apache.flink.streaming.api.checkpoint.CheckpointedFunction;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.sink.SinkFunction;

import java.time.Duration;
import java.util.ArrayList;
import java.util.List;

/**
 * 算子状态 模拟故障Sink数据
 */
public class BufferingSinkExample {
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

        stream.print("input ");

        //批量缓存输出
        stream.addSink(new BufferingSink(10));

        env.execute();
    }

    // 自定义实现SinkFunction

    public static class BufferingSink implements SinkFunction<Event>, CheckpointedFunction {
        // 定义阈值
        private final Integer threshold;

        public BufferingSink(Integer threshold) {
            this.threshold = threshold;
            this.bufferedElements = new ArrayList<>();
        }

        // 数据缓存队列
        private List<Event> bufferedElements;

        // 定义算子状态
        private ListState<Event> checkPointedState;

        @Override
        public void invoke(Event value, Context context) throws Exception {
            // 缓存到列表
            bufferedElements.add(value);
            // 判断达到阈值，就批量写入
            if (bufferedElements.size() == threshold) {
                System.out.println("=================开始输出=================");
                // 打印到控制台
                for (Event element : bufferedElements) {
                    System.out.println(element);
                }
                System.out.println("=================输出完毕=================");
                //清空队列
                bufferedElements.clear();
            }
        }

        @Override
        public void snapshotState(FunctionSnapshotContext context) throws Exception {
            // 复制前清空状态
            checkPointedState.clear();

            // 对状态进行持久化,复制缓存的列表到列表状态中
            checkPointedState.addAll(bufferedElements);
        }

        @Override
        public void initializeState(FunctionInitializationContext context) throws Exception {
            // 初始化状态
            ListStateDescriptor<Event> descriptor = new ListStateDescriptor<>("buffered-elements", Event.class);

            context.getOperatorStateStore().getListState(descriptor);

            // 如果从故障恢复，需要将ListState中的所有数据复制到列表中
            if (context.isRestored()) {
                for (Event event : checkPointedState.get()) {
                    bufferedElements.add(event);
                }
            }
        }
    }

}
