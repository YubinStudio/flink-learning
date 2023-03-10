package com.yu.wordcount;

import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.util.Collector;

public class StreamWordCount {
    public static void main(String[] args) throws Exception {
        // 1. 创建执行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        // 从参数中提取主机名和端口号
        // TODO:需要在idea <Run-Edit Configuration-program argument>中添加  --host localhost --port 9999
//        ParameterTool parameterTool = ParameterTool.fromArgs(args);
//        String host = parameterTool.get("host");
//        Integer port = parameterTool.getInt("port");

        // 2. 从socket端口读取文件
        /**
         * TODO:安装好naetcat后在CMD执行 # nc -l -p 9999
         * 并输入数据 Word
         */
        DataStreamSource<String> lineDataSteamSource = env.socketTextStream("hadoop01", 9999);
//        DataStreamSource<String> lineDataSteamSource = env.socketTextStream("localhost", 9999);
//        DataStreamSource<String> lineDataSteamSource = env.socketTextStream(host, port);

        // 3. 转换
        SingleOutputStreamOperator<Tuple2<String, Long>> wordAndOneTuple = lineDataSteamSource.flatMap((String line, Collector<Tuple2<String, Long>> out) -> {
            String[] words = line.split(" ");
            for (String word : words) {
                out.collect(Tuple2.of(word, 1L));

            }

        }).returns(Types.TUPLE(Types.STRING, Types.LONG));

        // 4. 分组    wordAndOneTuple.keyBy(0) --- 已弃用
        KeyedStream<Tuple2<String, Long>, String> wordAndOneTupleStream = wordAndOneTuple.keyBy(data -> data.f0);

        // 5.求和
        SingleOutputStreamOperator<Tuple2<String, Long>> result = wordAndOneTupleStream.sum(1);

        // 6.打印
        result.print();

        // 7.启动执行
        System.out.println("开始执行!");
        env.execute();

    }
}
