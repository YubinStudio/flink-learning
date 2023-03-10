package com.yu.table_sql;

import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.TableEnvironment;

import static org.apache.flink.table.api.Expressions.$;

public class CommonTableApi {

    public static void main(String[] args) {
        // 1. 定义环境配置来创建表
        // 基于blink版本planner进行流处理
        EnvironmentSettings settings = EnvironmentSettings.newInstance()
                .inStreamingMode()
                .useBlinkPlanner()
                .build();

        TableEnvironment tableEnv = TableEnvironment.create(settings);


        // 2. 创建表
        String createDDL = "CREATE TABLE clickTable (" +
                " user_name STRING," +
                " url STRING," +
                " ts BIGINT " +
                ") WITH (" +
                " 'connector' = 'filesystem', " +
                " 'path' = 'input/clicks.txt', " +
                " 'format' = 'csv' " +
                ")";
        tableEnv.executeSql(createDDL);


        // 3.1调用Table API进行表的查询转换
        Table clickTable = tableEnv.from("clickTable");
        Table resultTable = clickTable.where($("user_name").isEqual("Bob"))
                .select($("user_name"), $("url"));


        tableEnv.createTemporaryView("resultTab", resultTable);

        // 3.2 执行SQL进行表的查询转换
        Table resultTable2 = tableEnv.sqlQuery("select user_name, url from resultTab");

        // 3.3 执行聚合计算的查询转换
        Table aggResult = tableEnv.sqlQuery("select user_name,count(1) as cnt from clickTable group by user_name");

        // 4.1 创建输出表
        String createOutDDL = "CREATE TABLE outTable (" +
                " user_name STRING, " +
                " url STRING " +
                ") WITH (" +
                " 'connector' = 'filesystem', " +
                " 'path' = 'output-table', " +
                " 'format' =  'csv' " +
                ")";

        tableEnv.executeSql(createOutDDL);

        // 4.2 创建输出到控制台的表
        String createPrintOutDDL = "CREATE TABLE printOutTable (" +
                " user_name STRING, " +
                " cnt BIGINT " +
                ") WITH (" +
                " 'connector' = 'print'" +
//                " 'path' = 'output-table', " +
//                " 'format' =  'csv' " +
                ")";

        tableEnv.executeSql(createPrintOutDDL);

        // 输出表
        resultTable.executeInsert("outTable");
//        resultTable2.executeInsert("printOutTable");
        aggResult.executeInsert("printOutTable");

    }
}
