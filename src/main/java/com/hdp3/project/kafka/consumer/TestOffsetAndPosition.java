package com.hdp3.project.kafka.consumer;

import org.apache.kafka.clients.consumer.*;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.serialization.StringDeserializer;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Properties;

/**
 * @author Liuyongzhi
 * @description: 测试消息offse提交后，consumed offset、committed offset、position之间的关系
 * @date 2019/6/27
 */
public class TestOffsetAndPosition {

    private static final String BROKERLIST = "node71.xdata:6667,node72.xdata:6667,node73.xdata:6667";
    private static final String TOPIC = "topic-demo";
    private static final String GROUPID = "group.demo.test";

    private static Properties initConfig() {
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
        // 关闭offset自动提交
        props.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, false);
        return props;
    }

    public static void main(String[] args) {
        Properties props = initConfig();
        KafkaConsumer<String, String> kafkaConsumer = new KafkaConsumer<>(props);

        TopicPartition tp = new TopicPartition(TOPIC, 1);
        kafkaConsumer.assign(Arrays.asList(tp));

        long lastConsumerOffset = -1;
        List<ConsumerRecord<String, String>> partitionsRecords = new ArrayList<>();
        while (true) {
            ConsumerRecords<String, String> consumerRecords = kafkaConsumer.poll(5000);
            if (consumerRecords.isEmpty()) {
                System.out.println("records is " + consumerRecords.count() + " , 终止循环");
                break;
            }
            System.out.println("records is " + consumerRecords.count());
            for (ConsumerRecord<String, String> record : consumerRecords) {
                System.out.println("topic = " + record.topic() + ", partition = " + record.partition() + ", offset = " + record.offset());
                System.out.println("key = " + record.key() + ", value = " + record.value());
            }
            // 获取某一分区的所有消息记录
            partitionsRecords = consumerRecords.records(tp);
            // 获取当前消费的位置：通过得到最后一条消息的offset来获取。
            lastConsumerOffset = partitionsRecords.get(partitionsRecords.size() - 1).offset();
            // 同步提交消费位移
            kafkaConsumer.commitSync();
        }
        System.out.println("consumed offset is " + lastConsumerOffset);
        OffsetAndMetadata offsetAndMetadata = kafkaConsumer.committed(tp);
        System.out.println("committed offset is " + offsetAndMetadata.offset());
        long position = kafkaConsumer.position(tp);
        System.out.println("the offset of the next record is " + position);
        /*
         * 背景：
         *      消费topic-demo的1分区的消息，并同步提交。
         * 结果：
         *      consumed offset is 99
         *      committed offset is 100
         *      the offset of the next record is 100
         * 结论：
         *      position = committed offset = consumed offset + 1; 但 position 和 committed offset 并不会一直相同。
         */
    }
}
