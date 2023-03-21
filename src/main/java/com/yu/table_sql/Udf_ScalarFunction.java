package com.yu.table_sql;

import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.table.functions.ScalarFunction;

/**
 * 标量函数
 */
public class Udf_ScalarFunction {
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

        // 3.注册自定义标量函数
        tableEnv.createTemporarySystemFunction("MyHash", MyHashFunction.class);

        // 4.调用UDF进行查询转换
        Table resultTable = tableEnv.sqlQuery("select user_name,MyHash(user_name) from clickEventTable");

        // 5.转换成流输出
        tableEnv.toDataStream(resultTable).print("hash ");


        env.execute();
    }

    // 自定义标量函数
    public static class MyHashFunction extends ScalarFunction {
        public int eval(String str) {
            return str.hashCode();
        }
    }
}
