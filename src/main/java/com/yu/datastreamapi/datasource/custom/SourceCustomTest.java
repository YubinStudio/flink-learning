package com.yu.datastreamapi.datasource.custom;

import com.yu.pojo.Event;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

public class SourceCustomTest {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        //不能设置并行度 .setParallelism(2)
        //ClickSource是非并行的算子，并行度只能为1
        DataStreamSource<Event> clickSourceStream = env.addSource(new ClickSource());

        DataStreamSource<Integer> parallelClickSourceStream = env.addSource(new ParallelClickSource()).setParallelism(2);

//        clickSourceStream.print();
        parallelClickSourceStream.print();
        env.execute();

    }
}
