package com.yu.datastreamapi.sink;

import com.yu.pojo.Event;
import org.apache.flink.api.common.serialization.SimpleStringEncoder;
import org.apache.flink.core.fs.Path;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.sink.filesystem.StreamingFileSink;
import org.apache.flink.streaming.api.functions.sink.filesystem.rollingpolicies.DefaultRollingPolicy;

import java.util.concurrent.TimeUnit;

public class SinkToFileTest {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(4);

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

        // 1. 写入文件
        StreamingFileSink<Event> streamingFileSink = StreamingFileSink.<Event>forRowFormat(new Path("./output"),
                new SimpleStringEncoder<>("UTF-8"))
                .withRollingPolicy(
                        DefaultRollingPolicy.builder()
                                .withMaxPartSize(1024 * 1024 * 1024)        //文档1GB进行滚动
                                .withRolloverInterval(TimeUnit.MINUTES.toMillis(15))    //间隔15分钟滚动
                                .withInactivityInterval(TimeUnit.MINUTES.toMillis(5))   //间隔5分钟无数据直接落盘
                                .build()
                )
                .build();

        fromElements.addSink(streamingFileSink) ;
//        fromElements.map(data -> data.toString()).addSink(streamingFileSink);

        env.execute();
    }
}
