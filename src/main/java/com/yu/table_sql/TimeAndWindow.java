package com.yu.table_sql;

import com.yu.datastreamapi.datasource.custom.ClickSource;
import com.yu.pojo.Event;
import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;

import java.time.Duration;

import static org.apache.flink.table.api.Expressions.$;

/**
 * table-API和SQL简单使用
 * 时间、窗口、聚合查询、分组窗口聚合(已弃用 TODO Flink1.13之后使用窗口表值函数（Windowing TVFs，新版本）代替 )
 */
public class TimeAndWindow {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        StreamTableEnvironment tableEnv = StreamTableEnvironment.create(env);

        // 1. 在创建表的DDL语句语句中直接定义事件时间属性
        String createDDL = "CREATE TABLE clickTable ( " +
                " user_name STRING ," +
                " url STRING ," +
                " ts BIGINT , " +
                " et AS TO_TIMESTAMP(FROM_UNIXTIME(ts / 1000))," +
                " WATERMARK FOR et AS et - INTERVAL '1' SECOND " +
                ") WITH (" +
                " 'connector' = 'filesystem'," +
                " 'path' = 'input/clicks.txt'," +
                " 'format' = 'csv'" +
                ")";

        tableEnv.executeSql(createDDL);

        // 2. 在流转换成Table时定义事件时间属性
        SingleOutputStreamOperator<Event> clickStream = env.addSource(new ClickSource())
                .assignTimestampsAndWatermarks(WatermarkStrategy.<Event>forBoundedOutOfOrderness(Duration.ZERO)
                        .withTimestampAssigner(new SerializableTimestampAssigner<Event>() {
                            @Override
                            public long extractTimestamp(Event element, long recordTimestamp) {
                                return element.timestamp;
                            }
                        }));

        Table clickEvenTTable = tableEnv.fromDataStream(
                clickStream,
                $("user"),
                $("url"),
                $("timestamp").as("ts"),
                $("et").rowtime());

        tableEnv.createTemporaryView("clickEvenTTable", clickEvenTTable);
//        clickEvenTTable.printSchema();

        // 聚合查询转换
        // 1.分组查询
        Table aggCountTable = tableEnv.sqlQuery("select user_name,count(1) as cnt from clickTable group by user_name");


        // 2.分组窗口聚合; TODO Flink1.13之后使用窗口表值函数（Windowing TVFs，新版本）代替
        Table groupWindowTable = tableEnv.sqlQuery("select " +
                "   user_name,count(1) as cnt, " +
                "   TUMBLE_END(et, INTERVAL '10' SECOND) AS entTs" +
                " from clickTable" +
                " group by " +
                "   user_name," +
                "   TUMBLE(et ,INTERVAL '10' SECOND)"
        );

        // 3. 窗口聚合
        // 3.1 滚动窗口聚合
        Table tumbleWindowResultTable = tableEnv.sqlQuery(
                "select user_name ,count(1) AS cnt, " +
                        " window_end as endT, " +
                        " window_start as endS " +
                        "from TABLE( " +
                        " TUMBLE(TABLE clickTable, DESCRIPTOR(et), INTERVAL '10' SECOND )" +
                        ") " +
                        " GROUP BY user_name ,window_end,window_start"
        );

        // 3.1 滑动窗口聚合
        Table hopWindowResultTable = tableEnv.sqlQuery(
                "select user_name ,count(1) AS cnt, " +
                        " window_end as endT " +
                        "from TABLE( " +
                        " HOP(TABLE clickTable, DESCRIPTOR(et), INTERVAL '5' SECOND, INTERVAL '10' SECOND )" +
                        ") " +
                        " GROUP BY user_name ,window_end,window_start"
        );

        // 3.2 累计窗口
        Table cumulateWindowResultTable = tableEnv.sqlQuery(
                "select user_name ,count(1) AS cnt, " +
                        " window_end as endT " +
                        "FROM TABLE( " +
                        " CUMULATE(TABLE clickTable, DESCRIPTOR(et), INTERVAL '5' SECOND, INTERVAL '10' SECOND )" +
                        ") " +
                        " GROUP BY user_name ,window_end,window_start"
        );

        // 4. 开窗聚合（over）
        Table overWindow = tableEnv.sqlQuery(
                "select user_name, " +
                        " avg(ts) OVER(" +
                        " PARTITION BY user_name " +
                        " ORDER BY et " +
                        " ROWS BETWEEN 3 PRECEDING AND CURRENT ROW " +
                        ") AS avg_ts " +
                        "FROM clickTable"
        );

        Table overCountMaxWindow = tableEnv.sqlQuery(
                "select user_name, " +
                        " count(url) OVER w AS cnt ," +
                        " max(ts) OVER w AS max_ts " +
                        "FROM clickTable " +
                        " WINDOW w AS (" +
                        " PARTITION BY user_name ORDER BY et " +
                        " ROWS BETWEEN 2 PRECEDING AND CURRENT ROW )"
        );

//        tableEnv.toChangelogStream(aggCountTable).print("agg-count");
//        tableEnv.toDataStream(groupWindowTable).print("group Window ");
//        tableEnv.toDataStream(tumbleWindowResultTable).print("tumbleWindow ");
//        tableEnv.toDataStream(hopWindowResultTable).print("hopWindow ");
//        tableEnv.toDataStream(cumulateWindowResultTable).print("cumulateWindow ");
//        tableEnv.toDataStream(overWindow).print("overWindow ");
        tableEnv.toChangelogStream(overCountMaxWindow).print("overCountMaxWindow ");

        env.execute();
    }
}

