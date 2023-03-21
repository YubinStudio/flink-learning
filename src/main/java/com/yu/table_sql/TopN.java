package com.yu.table_sql;

import com.yu.datastreamapi.datasource.custom.ClickSource;
import com.yu.datastreamapi.datasource.custom.ClickSourceBounded;
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
 * 分组求TopN 选取当前所有用户中浏览量最大的2个
 */
public class TopN {
    public static void main(String[] args) throws Exception {
        // 1.创建执行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        StreamTableEnvironment tableEnv = StreamTableEnvironment.create(env);

        // 2.在创建表的DDL语句语句中直接定义事件时间属性
        String createDDL = "CREATE TABLE clickEventTable ( " +
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

        // 2.在流转换成Table时定义事件时间属性
//        SingleOutputStreamOperator<Event> clickStream = env.addSource(new ClickSourceBounded(10))
//                .assignTimestampsAndWatermarks(WatermarkStrategy.<Event>forBoundedOutOfOrderness(Duration.ZERO)
//                        .withTimestampAssigner(new SerializableTimestampAssigner<Event>() {
//                            @Override
//                            public long extractTimestamp(Event element, long recordTimestamp) {
//                                return element.timestamp;
//                            }
//                        }));

//        clickStream.print("clickStream:");

//        Table clickEventTable = tableEnv.fromDataStream(clickStream,
//                $("user").as("user_name"),
//                $("url"),
//                $("timestamp").as("ts"),
//                $("et").rowtime()
//        );

//        tableEnv.createTemporaryView("clickEventTable", clickEventTable);
//        tableEnv.toDataStream(tableEnv.sqlQuery("select * from clickEvenTTable")).print();

        // 普通Top N 选取当前所有用户中浏览量最大的2个
        Table topNResultTable = tableEnv.sqlQuery(
                "SELECT user_name, cnt ,row_num " +
                        " FROM (" +
                        "   SELECT *,ROW_NUMBER() OVER ( " +
                        "           ORDER BY cnt DESC" +
                        "       ) AS row_num" +
                        "   FROM (SELECT user_name, COUNT(1) AS cnt FROM clickEventTable GROUP BY user_name)" +
                        " ) WHERE row_num <=2 "
        );

        // 窗口Top N 选取当前所有用户中浏览量最大的2个
        String subQuery = "SELECT user_name,COUNT(1) AS cnt, window_start , window_end " +
                "FROM TABLE (" +
                "  TUMBLE(TABLE clickEventTable,DESCRIPTOR(et) ,INTERVAL '10' SECOND)" +
                ")" +
                "GROUP BY user_name, window_start , window_end ";

        Table windowTopNTable = tableEnv.sqlQuery(
                "SELECT user_name, cnt ,row_num ,window_end " +
                        " FROM (" +
                        "   SELECT *,ROW_NUMBER() OVER ( " +
                        "           PARTITION BY window_start, window_end " +
                        "           ORDER BY cnt DESC" +
                        "       ) AS row_num" +
                        "   FROM (" + subQuery + ")" +
                        " ) WHERE row_num <=2 "
        );


//        tableEnv.toChangelogStream(topNResultTable).print("TopN ");
        tableEnv.toChangelogStream(windowTopNTable).print("windowTopN ");
        env.execute();
    }
}
