package com.hdp.project.kafka.consumer;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.serialization.StringDeserializer;

import java.util.Arrays;
import java.util.HashSet;
import java.util.Properties;
import java.util.Set;

/**
 * @author Liuyongzhi
 * @description:
 * @date 2019/7/1
 */
public class SeekDemoAssignment {

    protected static final String BROKERLIST = "node71.xdata:6667,node72.xdata:6667,node73.xdata:6667";
    protected static final String TOPIC = "test";
    protected static final String GROUPID = "group.demo.222";

    protected static Properties initConfig() {
        Properties props = new Properties();
        // kafka集群所需的broker地址清单
        props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, BROKERLIST);
        // 设定kafkaConsumer对应的客户端id
        props.put(ConsumerConfig.CLIENT_ID_CONFIG, "consumer.client.id.demo");
        // 消费者从broker端获取的消息格式都是byte[]数组类型，key和value需要进行反序列化。
        props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        props.put(ConsumerConfig.GROUP_ID_CONFIG, GROUPID);
//        props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "none");
        return props;
    }

    public static void main(String[] args) {
        Properties props = initConfig();
        KafkaConsumer<String, String> consumer = new KafkaConsumer<>(props);
        consumer.subscribe(Arrays.asList(TOPIC));

        long start = System.currentTimeMillis();
        Set<TopicPartition> assignment = new HashSet<>();
        // 当分区消息为0时进入
        while (assignment.size() == 0) {
            consumer.poll(100);
            assignment = consumer.assignment();
        }
        long end = System.currentTimeMillis();
        System.out.println(end - start);
        System.out.println(assignment);

        for (TopicPartition tp : assignment) {
            System.out.println("#####################################");
            consumer.seek(tp, 0);
        }

        while (true) {
            ConsumerRecords<String, String> records = consumer.poll(1000);
            // 当拉取的记录为空时，终止循环
            if (records.isEmpty()) {
                System.out.println("!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!");
                break;
            }
            // 消费记录
            for (ConsumerRecord<String, String> record : records) {
                System.out.println(record.offset() + ":" + record.value() + ":" + record.partition());
            }
        }
    }
}
