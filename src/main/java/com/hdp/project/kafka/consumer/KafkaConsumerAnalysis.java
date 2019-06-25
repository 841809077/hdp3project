package com.hdp.project.kafka.consumer;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.common.PartitionInfo;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.serialization.StringDeserializer;

import java.lang.reflect.Array;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Properties;
import java.util.regex.Pattern;

/**
 * @author Liuyongzhi
 * @description: 消费者示例, 消费者会每隔1s向topic拉取消息，永远循环下去。
 * @date 2019/6/24
 */
public class KafkaConsumerAnalysis {

    private static final String BROKERLIST = "node71.xdata:6667,node72.xdata:6667,node73.xdata:6667";
    private static final String TOPIC = "test";
    private static final String GROUPID = "group.demo";

    public static Properties initConfig() {
        Properties props = new Properties();
        // kafka集群所需的broker地址清单
        props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, BROKERLIST);
        // 设定kafkaConsumer对应的客户端id
        props.put(ConsumerConfig.CLIENT_ID_CONFIG, "consumer.client.id.demo");
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
        // 可指定消费多个主题
//        kafkaConsumer.subscribe(Arrays.asList(TOPIC));
        // 以正则表达式匹配主题名称
//        kafkaConsumer.subscribe(Pattern.compile("topic-.*"));

        // 订阅test主题分区编号为0的分区
//        kafkaConsumer.assign(Arrays.asList(new TopicPartition(TOPIC, 0)));

        // 需求：通过partitionFor()和assign()来实现订阅主题所有分区
        // 1、assign()接收一个Collection<TopicPartition>，
        // 2、先获取指定主题的分区数，
        // 3、然后通过循环的形式将所有主题与分区的映射以new TopicPartition(topic,partition)的形式添加，传递给assign()
        List<PartitionInfo> lpi = kafkaConsumer.partitionsFor(TOPIC);
        List<TopicPartition> topicPartitions = new ArrayList<>();
        for (PartitionInfo pif : lpi) {
            topicPartitions.add(new TopicPartition(pif.topic(), pif.partition()));
        }
        kafkaConsumer.assign(topicPartitions);

        try {
            while (true) {
                ConsumerRecords<String, String> records = kafkaConsumer.poll(1000);
                for (ConsumerRecord<String, String> record : records) {
                    System.out.println("topic = " + record.topic() + ", partition = " + record.partition() + ", offset = " + record.offset());
                    System.out.println("key = " + record.key() + ", value = " + record.value());
                }
            }
        } catch (Exception e) {
            e.printStackTrace();
        } finally {
            kafkaConsumer.close();
        }
    }
}
