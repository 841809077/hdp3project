package com.hdp.project.kafka.producer;

import org.apache.kafka.clients.producer.ProducerInterceptor;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;

import java.util.Map;

/**
 * @author liuyzh
 * @description: 生产者的拦截器
 * @date 2019/6/19 13:08
 */
public class ProducerInterceptorPrefix implements ProducerInterceptor {

    private volatile long sendSuccess = 0;
    private volatile long sendFailure = 0;

    @Override
    public ProducerRecord onSend(ProducerRecord producerRecord) {
        String modifiedValue = "prefix1 - " + producerRecord.value();
        ProducerRecord<String, String> record = new ProducerRecord(producerRecord.topic(), producerRecord.partition(), producerRecord.timestamp(), producerRecord.key(), modifiedValue, producerRecord.headers());
        return record;
    }

    @Override
    public void onAcknowledgement(RecordMetadata recordMetadata, Exception e) {
        if (e == null) {
            sendSuccess++;
        } else {
            sendFailure++;
        }
    }

    @Override
    public void close() {
        double successRatio = (double) sendSuccess / (sendSuccess + sendFailure);
        System.out.println("发送成功率：" + String.format("%f", successRatio * 100) + "%");
    }

    @Override
    public void configure(Map<String, ?> map) {

    }
}
