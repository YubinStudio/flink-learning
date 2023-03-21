package com.yu.table_sql;

import com.yu.pojo.Event;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;

import static org.apache.flink.table.api.Expressions.$;

public class TableExample {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        // 读取数据源
        SingleOutputStreamOperator<Event> eventStream = env
                .fromElements(
                        new Event("Alice", "./home", 1000L),
                        new Event("Bob", "./cart", 1000L),
                        new Event("Alice", "./prod?id=1", 5 * 1000L),
                        new Event("Cary", "./home", 60 * 1000L),
                        new Event("Bob", "./prod?id=3", 90 * 1000L),
                        new Event("Alice", "./prod?id=7", 105 * 1000L)
                );
        // 获取表环境
        StreamTableEnvironment tableEnv = StreamTableEnvironment.create(env);

        // 将数据流转换成表
        Table eventTable = tableEnv.fromDataStream(eventStream);
        // 用执行 SQL 的方式提取数据
        Table visitTable1 = tableEnv.sqlQuery("select url, user, `timestamp` from " + eventTable);
        // 将表转换成数据流，打印输出
        tableEnv.toDataStream(visitTable1).print("Table1: ");

        // 用 Table API 方式提取数据
        Table visitTable2 = eventTable.select($("url"), $("user"))
                .where($("user").isEqual("Bob"));

        tableEnv.toDataStream(visitTable2).print("Table2: ");

        // 3.3 执行聚合计算的查询转换
        tableEnv.createTemporaryView("clickTable", eventTable);

        // 3.3 执行聚合计算的查询转换
        Table aggResult = tableEnv.sqlQuery("select user,count(1) as cnt from clickTable group by user");

        // 3.4 使用 toChangelogStream()转换为流
        tableEnv.toChangelogStream(aggResult).print("agg");

        // 执行程序
        env.execute();

    }
}
