package com.hdp.project.hdpproject.kafka;

import org.apache.kafka.clients.producer.*;
import org.springframework.boot.autoconfigure.kafka.KafkaProperties;

import java.util.Properties;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;

/**
 * @author Liuyongzhi
 * @description:
 * @date 2019/6/18 0018
 */
public class KafkaProducerAnalysis {

    private static final String brokerList = "node71.xdata:6667,node72.xdata:6667,node73.xdata:6667";
    private static final String topic = "test";

    private static Properties initConfig() {
        Properties props = new Properties();
        props.put("bootstrap.servers", brokerList);
        props.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        props.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        props.put("client.id", "producer.client.id.demo");
        props.put("retries", 3);
        // acks有三个匹配项，均为字符串类型，分别为："1"，"0","all或-1"
        props.put(ProducerConfig.ACKS_CONFIG, "1");
        return props;
    }

    // 方式一：发后即忘，性能高，可靠性差，易发生信息丢失。
    private static void fire_and_forget_send(Properties props) {
        KafkaProducer<String, String> producer = new KafkaProducer<>(props);
        ProducerRecord<String, String> record = new ProducerRecord<>(topic, "fire and forget send!");
        producer.send(record);
        producer.close();
    }

    // 方式二：同步发送消息，可靠性高，要么消息被发送成功，要么发生异常。如果发生异常，可以捕获并进行相应的处理。
    // 性能较发后即忘的方式差，需要阻塞等待一条消息发送完再发送下一条信息。
    private static void sync_send(Properties props) {
        KafkaProducer<String, String> producer = new KafkaProducer<>(props);
        ProducerRecord<String, String> record = new ProducerRecord<>(topic, "sync send!");
        try {
            // 方式一
//            producer.send(record).get();
            // 方式二
            Future<RecordMetadata> future = producer.send(record);  // future代表一个任务的声明周期。
            // 获取消息的元数据信息，比如当前消息的主题、分区号、分区中的偏移量（offset）、时间戳等。
            // 如果在应用代码中需要这些信息，可以使用这种方式。如果不需要，可采用方式一的写法。
            RecordMetadata metadata = future.get();
            System.out.println(metadata.topic() + " - " + metadata.partition() + " - " + metadata.offset());
        } catch (InterruptedException | ExecutionException e) {
            e.printStackTrace();
        }
        producer.close();
    }

    // 方式三：异步发送消息，增加一个回调函数。单纯的send()方法也是异步请求
    private static void async_send(Properties props) {
        KafkaProducer<String, String> producer = new KafkaProducer<>(props);
        ProducerRecord<String, String> record = new ProducerRecord<>(topic, "async send!");
        producer.send(record, new Callback() {
            @Override
            public void onCompletion(RecordMetadata recordMetadata, Exception e) {
                if (e != null) {
                    e.printStackTrace();
                } else {
                    System.out.println(recordMetadata.topic() + " - " + recordMetadata.partition() + " - " + recordMetadata.offset());
                }
            }
        });
        producer.close();
    }

    public static void main(String[] args) {
        Properties props = initConfig();
//        fire_and_forget_send(props);
//        sync_send(props);
        async_send(props);
    }

}
