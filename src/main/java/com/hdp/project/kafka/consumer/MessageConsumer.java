package com.hdp.project.kafka.consumer;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.PartitionInfo;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.serialization.StringDeserializer;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Iterator;
import java.util.List;
import java.util.Properties;

/**
 * @author liuyzh
 * @description: 消费者消费消息
 * @date 2019/7/12 13:00
 */
public class MessageConsumer {

    private static final String BROKERLIST = "node71.xdata:6667,node72.xdata:6667,node73.xdata:6667";
    private static final String TOPIC = "topic-demo";
    private static final String GROUPID = "1234";
    private static final String CLIENTID = "cetat";
    private KafkaConsumer<String, String> consumer;

    private static Properties initConfig() {
        Properties props = new Properties();
        // kafka集群所需的broker地址清单
        props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, BROKERLIST);
        // 设定kafkaConsumer对应的客户端id
        props.put(ConsumerConfig.CLIENT_ID_CONFIG, CLIENTID);
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

    private MessageConsumer() {
        Properties props = initConfig();
        consumer = new KafkaConsumer<>(props);
    }

    /**
     * @description: 使用 iterator() 和 foreach 来消费 poll() 到的数据
     * @return: void
     */
    private void way1() {
        // 订阅一个主题
        consumer.subscribe(Arrays.asList(TOPIC));
        try {
            while (true) {
                ConsumerRecords<String, String> records = consumer.poll(5000);
                System.out.println("##############################");
                System.out.println(records.count());

                // 当所有消息都已被消费完毕，则退出循环。
                // 但如果首次poll的数据为0，程序被终止。所以这里需要改进。
                if (records.isEmpty()) {
                    break;
                }

                // iterator()
                Iterator<ConsumerRecord<String, String>> iterator = records.iterator();
                while (iterator.hasNext()) {
                    ConsumerRecord<String, String> record = iterator.next();
                    System.out.println("topic = " + record.topic() + ", partition = " + record.partition() + ", offset = " + record.offset());
                    System.out.println("key = " + record.key() + ", value = " + record.value());
                }

                // foreach
//                for (ConsumerRecord<String, String> record : records) {
//                    System.out.println("topic = " + record.topic() + ", partition = " + record.partition() + ", offset = " + record.offset());
//                    System.out.println("key = " + record.key() + ", value = " + record.value());
//                }
            }
        } catch (Exception e) {
            e.printStackTrace();
        } finally {
            consumer.close();
        }
    }

    /**
     * @description: 按照分区消费
     * @return: void
     */
    public void way2() {
        // 订阅一个主题
        consumer.subscribe(Arrays.asList(TOPIC));

        try {
            while (true) {
                ConsumerRecords<String, String> records = consumer.poll(5000);
                System.out.println("##############################");
                System.out.println(records.count());

                // 当所有消息都已被消费完毕，则退出循环。
                // 但如果首次poll的数据为0，程序被终止。所以这里需要改进。
                if (records.isEmpty()) {
                    break;
                }

                // 使用records.records(TopicPartition)消费指定分区
                for (TopicPartition tp : records.partitions()) {
                    // tp: topic-demo-0、topic-demo-1、topic-demo-2、topic-demo-3
                    // 指定获取某一主题的某一分区：tp = new TopicPartition(TOPIC, 0)
                    for (ConsumerRecord<String, String> record : records.records(tp)) {
                        System.out.println("topic = " + record.topic() + ", partition = " + record.partition() + ", offset = " + record.offset());
                        System.out.println("key = " + record.key() + ", value = " + record.value());
                    }
                }
            }
        } catch (Exception e) {
            e.printStackTrace();
        } finally {
            consumer.close();
        }
    }

    /**
     * @description: 使用records(String topic)消费某一主题
     * @return: void
     */
    public void way3() {
        // 订阅两个主题
        consumer.subscribe(Arrays.asList("topic-demo", "topic-test"));

        try {
            while (true) {
                ConsumerRecords<String, String> records = consumer.poll(5000);
                System.out.println("##############################");
                System.out.println(records.count());

                // 当所有消息都已被消费完毕，则退出循环。
                // 但如果首次poll的数据为0，程序被终止。所以这里需要改进。
                if (records.isEmpty()) {
                    break;
                }

                // 使用records(String topic)只处理主题topic-demo的数据
                for(ConsumerRecord<String, String> record : records.records("topic-demo")){
                    System.out.println("topic = " + record.topic() + ", partition = " + record.partition() + ", offset = " + record.offset());
                    System.out.println("key = " + record.key() + ", value = " + record.value());
                }
            }
        } catch (Exception e) {
            e.printStackTrace();
        } finally {
            consumer.close();
        }
    }

    public static void main(String[] args) {
        MessageConsumer mc = new MessageConsumer();
        mc.way3();
    }

}
