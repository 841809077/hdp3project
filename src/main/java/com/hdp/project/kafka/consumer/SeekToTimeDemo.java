package com.hdp.project.kafka.consumer;

import org.apache.kafka.clients.consumer.*;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.serialization.StringDeserializer;

import java.util.*;

/**
 * @author Liuyongzhi
 * @description: 获取2019.07.21 18:00之后的消息
 * @date 2019/7/21
 */
public class SeekToTimeDemo {

    private static final String BROKERLIST = "node71.xdata:6667,node72.xdata:6667,node73.xdata:6667";
    private static final String TOPIC = "topic-demo";
    private static final String GROUPID = "6666";
    private static final String CLIENTID = "888";

    private static Properties initConfig() {
        Properties props = new Properties();
        // kafka集群所需的broker地址清单
        props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, BROKERLIST);
        // 设定kafkaConsumer对应的客户端id
        props.put(ConsumerConfig.CLIENT_ID_CONFIG, CLIENTID);
        // 消费者从broker端获取的消息格式都是byte[]数组类型，key和value需要进行反序列化。
        props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        // 指定消费者组
        props.put(ConsumerConfig.GROUP_ID_CONFIG, GROUPID);
        return props;
    }

    public static void main(String[] args) {
        Properties props = initConfig();
        KafkaConsumer<String, String> consumer = new KafkaConsumer<>(props);
        consumer.subscribe(Arrays.asList(TOPIC));

        Set<TopicPartition> assignment = new HashSet<>();
        // 在poll()方法内部执行分区分配逻辑，该循环确保分区已被分配。
        // 当分区消息为0时进入此循环，如果不为0，则说明已经成功分配到了分区。
        while (assignment.size() == 0) {
            consumer.poll(100);
            // assignment()方法是用来获取消费者所分配到的分区消息的
            // assignment的值为：topic-demo-3, topic-demo-0, topic-demo-2, topic-demo-1
            assignment = consumer.assignment();
        }
        System.out.println(assignment);

        Map<TopicPartition, Long> timestampToSearch = new HashMap<>();
        for (TopicPartition tp : assignment) {
            // 设置查询分区时间戳的条件：获取当前时间前一天之后的消息
            timestampToSearch.put(tp, System.currentTimeMillis() - 24 * 3600 * 1000);
        }

        // timestampToSearch的值为{topic-demo-0=1563709541899, topic-demo-2=1563709541899, topic-demo-1=1563709541899}
        Map<TopicPartition, OffsetAndTimestamp> offsets = consumer.offsetsForTimes(timestampToSearch);

        for(TopicPartition tp: assignment){
            // 获取该分区的offset以及timestamp
            OffsetAndTimestamp offsetAndTimestamp = offsets.get(tp);
            // 如果offsetAndTimestamp不为null，则证明当前分区有符合时间戳条件的消息
            if (offsetAndTimestamp != null) {
                consumer.seek(tp, offsetAndTimestamp.offset());
            }
        }

        while (true) {
            ConsumerRecords<String, String> records = consumer.poll(1000);

            System.out.println("##############################");
            System.out.println(records.count());

            // 当拉取的记录为空时，终止循环
            //            if (records.isEmpty()) {
            //                break;
            //            }

            // 消费记录
            for (ConsumerRecord<String, String> record : records) {
                System.out.println(record.offset() + ":" + record.value() + ":" + record.partition() + ":" + record.timestamp());
            }
        }
    }
}
