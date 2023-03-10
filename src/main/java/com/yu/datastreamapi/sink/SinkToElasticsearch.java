package com.yu.datastreamapi.sink;

import com.yu.pojo.Event;
import org.apache.flink.api.common.functions.RuntimeContext;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.connectors.elasticsearch.ElasticsearchSinkFunction;
import org.apache.flink.streaming.connectors.elasticsearch.RequestIndexer;
import org.apache.flink.streaming.connectors.elasticsearch7.ElasticsearchSink;
import org.apache.http.HttpHost;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.client.Requests;

import java.util.ArrayList;
import java.util.HashMap;

public class SinkToElasticsearch {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        //从元素中读取数据
        DataStreamSource<Event> fromElements = env.fromElements(
                new Event("Tom", "./cart", 10000L),
                new Event("Judy", "./home", 20000L),
                new Event("Martin", "./home", 4000L)
        );


        //创建es连接配置
        ArrayList<HttpHost> httpHosts = new ArrayList<>();
        httpHosts.add(new HttpHost("hadoop01", 9200));

        //定义 ElasticsearchSinkFunction
        ElasticsearchSinkFunction<Event> elasticsearchSinkFunction = new ElasticsearchSinkFunction<Event>() {
            @Override
            public void process(Event event, RuntimeContext runtimeContext, RequestIndexer requestIndexer) {
                HashMap<String, String> map = new HashMap<>();
                map.put(event.user, event.url);

                // 构建一个 IndexRequest
                IndexRequest request = Requests.indexRequest()
                        .index("events")
                        .type("type")
                        .source(map);
                requestIndexer.add(request);
            }
        };

        /**
         *写入 Elasticsearch
         * 1 启动Elasticsearch
         *      ./bin/elasticsearch
         * 2 启动程序
         *
         * 3 启动redis客户端查看数据
         *      curl "localhost:9200/_cat/indices?v"
         *      curl "localhost:9200/events/_search?pretty"
         *
         */
        fromElements.addSink(new ElasticsearchSink.Builder<>(httpHosts, elasticsearchSinkFunction).build());

        env.execute();
    }

//自定义类实现redisMapper接口

}
