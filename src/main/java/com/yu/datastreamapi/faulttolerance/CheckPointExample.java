package com.yu.datastreamapi.faulttolerance;

import com.yu.datastreamapi.transformer.TransformMapTest;
import com.yu.pojo.Event;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.streaming.api.CheckpointingMode;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.CheckpointConfig;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

public class CheckPointExample {

    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        // ---------------------------------------------------------------//
        // TODO 配置检查点
        // 检查点分界线插入数据流的时间间隔
        env.enableCheckpointing(1000L);
        CheckpointConfig checkpointConfig = env.getCheckpointConfig();
        // 设置超时时间 ,超时的检查点就直接被启用，设置1分钟 适用实时性要求不高的场景
        checkpointConfig.setCheckpointTimeout(60000L);
        //
        checkpointConfig.setCheckpointingMode(CheckpointingMode.AT_LEAST_ONCE);
        // 前后两个检查点 时间间隔
        checkpointConfig.setMinPauseBetweenCheckpoints(500L);
        // 最大并发的检查点数
        checkpointConfig.setMaxConcurrentCheckpoints(1);
        // 开启不对齐的检查点操作，不需要等待所有barrier到达下游任务；只有并发数量为1的时候开启
        checkpointConfig.enableUnalignedCheckpoints();

        // ---------------------------------------------------------------//

        //从元素中读取数据
        SingleOutputStreamOperator<Event> fromElements = env.fromElements(
                new Event("Tom", "./cart", 1000L),
                new Event("Judy", "./home", 2000L),
                new Event("Martin", "./home", 4000L)
        ).uid("source-elements-001");
        // 1 .使用自定义类,实现MapFunction; map提取出user
        SingleOutputStreamOperator<String> userCustom = fromElements.map(new TransformMapTest.MyMapper());

        // 2. 使用匿名内部类实现MapFunction; map提取出user
        SingleOutputStreamOperator<String> userAnonymous = fromElements.map(new MapFunction<Event, String>() {
            @Override
            public String map(Event value) throws Exception {
                return value.user;
            }
        });
        // 3. 使用 Lambda ; map提取出user
        SingleOutputStreamOperator<String> userLambda = fromElements.map(data -> data.user);

        userCustom.print("1");
        userAnonymous.print("2");
        userLambda.print("3");

        env.execute();
    }

}
