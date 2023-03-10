package com.yu.datastreamapi.datasource;

import com.yu.pojo.Event;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

import java.util.ArrayList;

/**
 * 从文件、集合、对象、socket读取数据
 */
public class SourceTest {
    public static void main(String[] args) throws Exception {
        // 创建环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        //1 从文件读取
        DataStreamSource<String> fromFile = env.readTextFile("input\\clicks.txt");

        //2 从集合读取
        ArrayList<Integer> nums = new ArrayList<>();
        nums.add(29);
        nums.add(99);
        DataStreamSource<Integer> fromCollection = env.fromCollection(nums);

        //3 从对象读取
        DataStreamSource<Event> fromElements = env.fromElements(
                new Event("Tom", "./cart", 10000L),
                new Event("Judy", "./home", 20000L)
        );

        //4 从socket流中读取数据
//        DataStreamSource<String> fromSocket = env.socketTextStream("localhost", 8888);

        // 打印
        fromFile.print("m1");
        fromCollection.print("m2");
        fromElements.print("m3");
//        fromSocket.print("m4");



        env.execute();
    }
}
