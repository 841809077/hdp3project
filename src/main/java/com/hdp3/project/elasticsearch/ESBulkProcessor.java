package com.hdp3.project.elasticsearch;

import org.elasticsearch.action.bulk.BackoffPolicy;
import org.elasticsearch.action.bulk.BulkProcessor;
import org.elasticsearch.action.bulk.BulkRequest;
import org.elasticsearch.action.bulk.BulkResponse;
import org.elasticsearch.action.delete.DeleteRequest;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.client.transport.TransportClient;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.transport.TransportAddress;
import org.elasticsearch.common.unit.ByteSizeUnit;
import org.elasticsearch.common.unit.ByteSizeValue;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentFactory;
import org.elasticsearch.transport.client.PreBuiltTransportClient;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.net.InetAddress;
import java.net.UnknownHostException;
import java.util.HashMap;
import java.util.Map;
import java.util.Random;
import java.util.concurrent.TimeUnit;

/**
 * @author liuyzh
 * @description: BulkProcessor的具体实现（批量增删）
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
                        logger.info("序号：{} ，开始执行 {} 条数据批量操作。", executionId, request.numberOfActions());
                    }

                    @Override
                    public void afterBulk(long executionId,
                                          BulkRequest request,
                                          BulkResponse response) {
                        // 在每次执行BulkRequest后调用，通过此方法可以获取BulkResponse是否包含错误
                        if (response.hasFailures()) {
                            logger.error("Bulk {} executed with failures", executionId);
                        } else {
                            logger.info("序号：{} ，执行 {} 条数据批量操作成功，共耗费{}毫秒。", executionId, request.numberOfActions(), response.getTook().getMillis());
                        }
                    }

                    @Override
                    public void afterBulk(long executionId,
                                          BulkRequest request,
                                          Throwable failure) {
                        logger.error("序号：{} 批量操作失败，总记录数：{} ，报错信息为：{}", executionId, request.numberOfActions(), failure.getMessage());
                    }
                })
                // 每添加1000个request，执行一次bulk操作
                .setBulkActions(1000)
                // 每达到5M的请求size时，执行一次bulk操作
                .setBulkSize(new ByteSizeValue(5, ByteSizeUnit.MB))
                // 每5s执行一次bulk操作
                .setFlushInterval(TimeValue.timeValueSeconds(5))
                // 设置并发请求数。默认是1，表示允许执行1个并发请求，积累bulk requests和发送bulk是异步的，其数值表示发送bulk的并发线程数（可以为2、3、...）；若设置为0表示二者同步。
                .setConcurrentRequests(1)
                // 最大重试次数为3次，启动延迟为100ms。
                .setBackoffPolicy(
                        BackoffPolicy.exponentialBackoff(TimeValue.timeValueMillis(100), 3))
                .build();
    }

    /**
     * @return void
     * @description: 使用bulkProcessor批量插入map数据
     */
    private void mapData() {
        ESBulkProcessor esBulkProcessor = new ESBulkProcessor();
        BulkProcessor esBulk = esBulkProcessor.bulkProcessor();
        Map<String, Object> m = new HashMap<>();
        for (int i = 0; i < 3000; i++) {
            m.put("name", "name" + i);
            m.put("age", new Random().nextInt(50));
            m.put("sex", Math.round(Math.random()) == 1 ? "男" : "女");
            esBulk.add(new IndexRequest("es_map_test", "_doc", String.valueOf(i)).source(m));
        }
        // 最后执行一次刷新操作
        esBulk.flush();
        // 30秒后关闭BulkProcessor
        try {
            esBulk.awaitClose(30, TimeUnit.SECONDS);
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
    }

    /**
     * @return void
     * @description: 使用bulkProcessor批量插入json数据
     */
    private void jsonData() {
        ESBulkProcessor esBulkProcessor = new ESBulkProcessor();
        BulkProcessor esBulk = esBulkProcessor.bulkProcessor();
        for (int i = 0; i < 3000; i++) {
            try {
                XContentBuilder builder = XContentFactory.jsonBuilder()
                        .startObject()
                        .field("name", "name" + i)
                        .field("age", new Random().nextInt(50))
                        .field("sex", Math.round(Math.random()) == 1 ? "男" : "女")
                        .endObject();
                // 如果json是String类型的话，xx.source(jsonString, XContentType.JSON)
                esBulk.add(new IndexRequest("es_json_test", "_doc", String.valueOf(i)).source(builder));
            } catch (IOException e) {
                e.printStackTrace();
            }
        }
        // 最后执行一次刷新操作
        esBulk.flush();
        // 30秒后关闭BulkProcessor
        try {
            esBulk.awaitClose(30, TimeUnit.SECONDS);
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
    }

    /**
     * @return void
     * @description: 使用bulkProcessor批量数据
     */
    private void bulkDelete() {
        ESBulkProcessor esBulkProcessor = new ESBulkProcessor();
        BulkProcessor esBulk = esBulkProcessor.bulkProcessor();
        for (int i = 0; i < 3000; i++) {
            esBulk.add(new DeleteRequest("es_map_test", "_doc", String.valueOf(i)));
        }
        // 最后执行一次刷新操作
        esBulk.flush();
        // 30秒后关闭BulkProcessor
        try {
            esBulk.awaitClose(30, TimeUnit.SECONDS);
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
    }

    public static void main(String[] args) {
        ESBulkProcessor esBulkProcessor = new ESBulkProcessor();
//        esBulkProcessor.mapData();
//        esBulkProcessor.jsonData();
        esBulkProcessor.bulkDelete();
    }
}
