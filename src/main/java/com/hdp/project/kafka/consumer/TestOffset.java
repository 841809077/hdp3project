package com.hdp.project.kafka.consumer;

import org.apache.kafka.clients.consumer.*;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.serialization.StringDeserializer;

import java.util.ArrayList;
import java.util.List;
import java.util.Properties;

/**
 * @author Liuyongzhi
 * @description:
 * @date 2019/6/27
 */
public class TestOffset {

    public static final String BROKERLIST = "node71.xdata:6667,node72.xdata:6667,node73.xdata:6667";
    public static final String TOPIC = "test";
    public static final String GROUPID = "group.demo.4321";

    public static Properties initConfig() {
        Properties props = new Properties();
        // kafka集群所需的broker地址清单
        props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, BROKERLIST);
        // 设定kafkaConsumer对应的客户端id
        props.put(ConsumerConfig.CLIENT_ID_CONFIG, "consumer.client.id.demo11");
        // 消费者从broker端获取的消息格式都是byte[]数组类型，key和value需要进行反序列化。
        props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        // 指定一个全新的group.id并且将auto.offset.reset设置为earliest可拉取该主题内所有消息记录。
        props.put(ConsumerConfig.GROUP_ID_CONFIG, GROUPID);
        props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        return props;
    }

    public static void main(String[] args) {
        Properties props = initConfig();
        KafkaConsumer<String, String> kafkaConsumer = new KafkaConsumer<>(props);

        TopicPartition tp = new TopicPartition("test", 3);
        List<TopicPartition> topicPartitions = new ArrayList<>();
        topicPartitions.add(tp);
        kafkaConsumer.assign(topicPartitions);

        long lastConsumerOffset = -1;
        List<ConsumerRecord<String, String>> partitionsRecords = new ArrayList<>();
        while (true) {
            ConsumerRecords<String, String> consumerRecords = kafkaConsumer.poll(1000);
            if (consumerRecords.isEmpty()) {
                break;
            }
            for (ConsumerRecord<String, String> record : consumerRecords) {
                System.out.println("topic = " + record.topic() + ", partition = " + record.partition() + ", offset = " + record.offset());
                System.out.println("key = " + record.key() + ", value = " + record.value());
            }

            // 获取某一分区的所有消息记录
            partitionsRecords = consumerRecords.records(tp);
            // 获取当前消费的位置：通过得到最后一条消息的offset来获取。
            lastConsumerOffset = partitionsRecords.get(partitionsRecords.size() - 1).offset();
//            // 同步提交消费位移
//            consumer.commitSync();
        }

        System.out.println(partitionsRecords.size());

        System.out.println("consumed offset is " + lastConsumerOffset);
        OffsetAndMetadata offsetAndMetadata = kafkaConsumer.committed(tp);
        System.out.println("committed offset is " + offsetAndMetadata.offset());
        long position = kafkaConsumer.position(tp);
        System.out.println(" the  offset  of  t he  next  record  is " + position);

    }

}
