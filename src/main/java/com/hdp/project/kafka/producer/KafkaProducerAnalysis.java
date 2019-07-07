package com.hdp.project.kafka.producer;

import org.apache.kafka.clients.producer.*;
import org.apache.kafka.common.PartitionInfo;

import java.util.List;
import java.util.Properties;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;

/**
 * @author Liuyongzhi
 * @description: 生产者发送消息的三种方式
 * @date 2019/6/18
 */
public class KafkaProducerAnalysis {

    private static final String BROKERLIST = "node71.xdata:6667,node72.xdata:6667,node73.xdata:6667";
    private static final String TOPIC = "topic-demo";

    private static Properties initConfig() {
        Properties props = new Properties();
        props.put("bootstrap.servers", BROKERLIST);
        props.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        props.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        props.put("client.id", "producer.client.id.demo");
        props.put("retries", 3);
        // acks有三个匹配项，均为字符串类型，分别为："1"，"0","all或-1"。
        props.put(ProducerConfig.ACKS_CONFIG, "1");
        // 指定拦截器
        props.put(ProducerConfig.INTERCEPTOR_CLASSES_CONFIG, ProducerInterceptorPrefix.class.getName());
        return props;
    }

    /**
     * @description: 方式一：发后即忘，性能高，可靠性差，易发生信息丢失。
     * 如果没有指定分区号，在ProducerRecord里面指定每条消息的key值，会根据key值来判断发往哪个分区。
     * 如果指定分区号，会忽略对key值得判断，直接将消息发送到指定分区。
     * @param: props
     * @return: void
     */
    private static void fireAndForgetSend(Properties props, int count) {
        KafkaProducer<String, String> producer = new KafkaProducer<>(props);
        ProducerRecord<String, String> record = null;
        List<PartitionInfo> partitions = producer.partitionsFor(TOPIC);
        // 得到主题的分区数
        int numPartitions = partitions.size();
        System.out.println(numPartitions);
        for (int i = 1; i <= count; i++) {
            String messageStr = "测试，这是第" + i + "条数据";
            // 当指定发送消息的分区时，程序就不会根据key值再判断发往哪个分区了。
            record = new ProducerRecord<>(TOPIC, 1, String.valueOf(i), messageStr);
            //生产者发布消息到KAFKA_TEST，若Topic不存在则自动创建。
            producer.send(record);
            System.out.println(messageStr);
            try {
                // 时间间隔1s
                Thread.sleep(1000);
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
        }
        producer.close();
    }

    /**
     * @description: 方式二：同步发送消息，可靠性高，要么消息被发送成功，要么发生异常。如果发生异常，可以捕获并进行相应的处理。
     * 性能较"发后即忘"的方式差，需要阻塞等待一条消息发送完再发送下一条信息。
     * @param: props
     * @return: void
     */
    private static void syncSend(Properties props) {
        KafkaProducer<String, String> producer = new KafkaProducer<>(props);
        ProducerRecord<String, String> record = new ProducerRecord<>(TOPIC, "sync send!");
        try {
            // 方式一
//            producer.send(record).get();
            // 方式二
            // future代表一个任务的声明周期。
            Future<RecordMetadata> future = producer.send(record);
            // 获取消息的元数据信息，比如当前消息的主题、分区号、分区中的偏移量（offset）、时间戳等。
            // 如果在应用代码中需要这些信息，可以使用这种方式。如果不需要，可采用方式一的写法。
            RecordMetadata metadata = future.get();
            System.out.println(metadata.topic() + " - " + metadata.partition() + " - " + metadata.offset());
        } catch (InterruptedException | ExecutionException e) {
            e.printStackTrace();
        }
        producer.close();
    }

    /**
     * @description: 方式三：异步发送消息，增加一个回调函数。单纯的send()方法也是异步请求。
     * @param: props
     * @return: void
     */
    private static void asyncSend(Properties props) {
        KafkaProducer<String, String> producer = new KafkaProducer<>(props);
        ProducerRecord<String, String> record = new ProducerRecord<>(TOPIC, "async send!");
        producer.send(record, (recordMetadata, e) -> {
            if (e != null) {
                e.printStackTrace();
            } else {
                System.out.println(recordMetadata.topic() + " - " + recordMetadata.partition() + " - " + recordMetadata.offset());
            }
        });
        producer.close();
    }

    public static void main(String[] args) {
        Properties props = initConfig();
        fireAndForgetSend(props, 100);
//        syncSend(props);
//        asyncSend(props);
    }

}
