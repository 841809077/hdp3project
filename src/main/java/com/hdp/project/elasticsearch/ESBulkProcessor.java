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

    protected BulkProcessor bulkProcessor() {

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
                        System.out.println("---尝试操作" + request.numberOfActions() + "条数据---");
                    }

                    @Override
                    public void afterBulk(long executionId,
                                          BulkRequest request,
                                          BulkResponse response) {
                        System.out.println("---尝试操作" + request.numberOfActions() + "条数据失败---");
                    }

                    @Override
                    public void afterBulk(long executionId,
                                          BulkRequest request,
                                          Throwable failure) {
                        System.out.println("---尝试操作" + request.numberOfActions() + "条数据成功---");
                    }
                })
                .setBulkActions(1000)
                .setBulkSize(new ByteSizeValue(5, ByteSizeUnit.MB))
                .setFlushInterval(TimeValue.timeValueSeconds(5))
                .setConcurrentRequests(5)
                .setBackoffPolicy(
                        BackoffPolicy.exponentialBackoff(TimeValue.timeValueMillis(100), 3))
                .build();


    }

    public static void main(String[] args) {
        ESBulkProcessor esBulkProcessor = new ESBulkProcessor();
        for (int i = 0; i < 500; i++) {
            // 添加单次请求
            Map<String, Object> m = new HashMap<>();
            m.put("test2", i + "ceshi");
            esBulkProcessor.bulkProcessor().add(new IndexRequest("666", "_doc", String.valueOf(i)).source(m));
        }
        // 添加单次请求
//        Map<String, Object> m = new HashMap<>();
//        m.put("test2", "ceshi123");
//        esBulkProcessor.bulkProcessor().add(new IndexRequest("index123", "_doc", String.valueOf(111)).source(m));
        esBulkProcessor.bulkProcessor().flush();
        esBulkProcessor.bulkProcessor().close();

    }

}
