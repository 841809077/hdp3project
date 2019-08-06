package com.hdp.project.elasticsearch;

import org.elasticsearch.action.bulk.BackoffPolicy;
import org.elasticsearch.action.bulk.BulkProcessor;
import org.elasticsearch.action.bulk.BulkRequest;
import org.elasticsearch.action.bulk.BulkResponse;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.client.transport.TransportClient;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.transport.TransportAddress;
import org.elasticsearch.common.unit.ByteSizeUnit;
import org.elasticsearch.common.unit.ByteSizeValue;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.transport.client.PreBuiltTransportClient;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.net.InetAddress;
import java.net.UnknownHostException;
import java.util.HashMap;
import java.util.Map;

/**
 * @author liuyzh
 * @description:
 * @date 2019/8/5 16:40
 */
public class ESBulkProcessor {

    private static final Logger logger = LoggerFactory.getLogger(ESBulkProcessor.class);

    private final static String HOST = "10.6.6.73";
    private final static int PORT = 9300;
    private TransportClient client;

    private BulkProcessor bulkProcessor() {

        // 设置集群名称
        Settings settings = Settings.builder().put("cluster.name", "elasticsearch").build();

        // 创建客户端
        try {
            client = new PreBuiltTransportClient(settings)
                    .addTransportAddresses(new TransportAddress(InetAddress.getByName(HOST), PORT));
        } catch (UnknownHostException e) {
            logger.error(e.getMessage());
        }

        return BulkProcessor.builder(
                client,
                new BulkProcessor.Listener() {
                    @Override
                    public void beforeBulk(long executionId,
                                           BulkRequest request) {
                        System.out.println("序号：" + executionId + "，开始执行" + request.numberOfActions() + "条数据批量插入");
                    }

                    @Override
                    public void afterBulk(long executionId,
                                          BulkRequest request,
                                          BulkResponse response) {
                        System.out.println("序号：" + executionId + "，执行" + request.numberOfActions() + "条数据批量插入成功");
                    }

                    @Override
                    public void afterBulk(long executionId,
                                          BulkRequest request,
                                          Throwable failure) {
                        System.out.println("序号：" + executionId + " 执行失败，总记录数：" + request.numberOfActions() + "，报错信息为：" + failure.getMessage());
                    }
                })
                .setBulkActions(1000)
                .setBulkSize(new ByteSizeValue(5, ByteSizeUnit.MB))
                .setFlushInterval(TimeValue.timeValueSeconds(20))
                .setConcurrentRequests(1)
                .setBackoffPolicy(
                        BackoffPolicy.exponentialBackoff(TimeValue.timeValueMillis(100), 3))
                .build();
    }

    public static void main(String[] args) {
        ESBulkProcessor esBulkProcessor = new ESBulkProcessor();
        BulkProcessor esBulk = esBulkProcessor.bulkProcessor();
        Map<String, Object> m = new HashMap<>();
        for (int i = 5000; i < 10000; i++) {
            m.clear();
            m.put("test2", i + "ceshi");
            // 添加单次请求
            esBulk.add(new IndexRequest("ceshi", "_doc", String.valueOf(i)).source(m));
        }
        esBulk.flush();
        esBulk.close();
    }
}
