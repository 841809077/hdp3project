package com.hdp.project.hdpproject.kafka;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;

import java.util.Properties;
import java.util.concurrent.ExecutionException;

/**
 * @author Liuyongzhi
 * @description: 自定义生产者，结合自定义序列化器。
 * @date 2019/6/21 0021
 */
public class ProducerSelfSerializer {

    private static final String BROKERLIST = "node71.xdata:6667,node72.xdata:6667,node73.xdata:6667";
    private static final String TOPIC = "test";

    private static Properties initConfig() {
        Properties props = new Properties();
        props.put("bootstrap.servers", BROKERLIST);
        props.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        // 自定义序列化器
        props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, CompanySerializer.class.getName());
        props.put("client.id", "producer.client.id.demo");
        props.put("retries", 3);
        // acks有三个匹配项，均为字符串类型，分别为："1"，"0","all或-1"。
        props.put(ProducerConfig.ACKS_CONFIG, "1");
        return props;
    }

    public static void main(String[] args) {
        Properties properties = initConfig();
        KafkaProducer<String, Company> producer = new KafkaProducer<>(properties);
        // 数据消息
        Company company = Company.builder().name("xiaoliang").address("shandongqingdao").build();
        ProducerRecord<String, Company> record = new ProducerRecord<>(TOPIC, company);
        try {
            // 经过尝试，必须指定.get()，不指定的话，虽然程序不报错，但数据生产不成功。
            producer.send(record).get();
        } catch (InterruptedException | ExecutionException e) {
            e.printStackTrace();
        }
    }

}
