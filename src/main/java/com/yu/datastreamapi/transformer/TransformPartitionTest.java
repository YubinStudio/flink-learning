package com.yu.datastreamapi.transformer;

import com.yu.pojo.Event;
import org.apache.flink.api.common.functions.Partitioner;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.source.RichParallelSourceFunction;

public class TransformPartitionTest {
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
                new Event("Martin", "./cart", 6000L),
                new Event("Martin", "./prod", 7000L)
        );

        // 1 随机分区 shuffle
//        fromElements.shuffle().print().setParallelism(4);

        // 2 轮询分区 rebalance
//        fromElements.rebalance().print().setParallelism(4);

        // 3 重缩放分区
        env.addSource(new RichParallelSourceFunction<Integer>() {
            @Override
            public void run(SourceContext<Integer> ctx) throws Exception {
                for (int i = 1; i <= 8; i++) {
                    // 将奇偶数分别发送到0号和1号并行分区
                    if (i % 2 == getRuntimeContext().getIndexOfThisSubtask())
                        ctx.collect(i);
                }
            }

            @Override
            public void cancel() {

            }
        }).setParallelism(2)
//                .rescale()
//                .print()
                .setParallelism(4);


        // 4 广播 --分发到下游所有分区，会导致重复消费，只有在特定情况使用
//        fromElements.broadcast().print().setParallelism(4);


        // 5 全局分区 把所有数据发送到一个分区; 特殊场景：上游已经做过分区聚合的操作且数据量较少  可以发送到一个分区
//        fromElements.global().print().setParallelism(4);

        // 6 自定义重分区
        env.fromElements(1, 2, 3, 4, 5, 6, 7, 8)
                .partitionCustom(new Partitioner<Integer>() {
                                     @Override
                                     public int partition(Integer key, int numPartitions) {
                                         return key % 2;
                                     }
                                 }, new KeySelector<Integer, Integer>() {
                                     @Override
                                     public Integer getKey(Integer value) throws Exception {
                                         return value;
                                     }
                                 }
                ).print().setParallelism(4);


        env.execute();
    }
}
