package com.github.somendra87.tutorial1.consumer;

import com.github.somendra87.KafkaConstants;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.util.Arrays;
import java.util.Properties;

/**
 * @author somendraprakash created on 16/12/21
 */
public class ConsumerDemo
{
    private static final Logger log = LoggerFactory.getLogger(ConsumerDemo.class.getName());
    private String groupId = "my-fourth-application";
    private String topic = "first_topic";

    // create consumer configs
    private Properties getConsumerProperties() {
        Properties properties = new Properties();
        properties.setProperty(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, KafkaConstants.LOCALHOST_9092);
        properties.setProperty(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        properties.setProperty(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        properties.setProperty(ConsumerConfig.GROUP_ID_CONFIG, groupId);
        properties.setProperty(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        return properties;
    }

    //create consumer
    private KafkaConsumer<String, String> consumer(){
        KafkaConsumer<String,String> consumer = new KafkaConsumer<String, String>(getConsumerProperties());

        // subscribe consumer to our topic
        consumer.subscribe(Arrays.asList(topic));

        return consumer;
    }

    //poll for new data
    private void pollForNewData(){
        KafkaConsumer<String, String> consumer = consumer();
        while (true){
            ConsumerRecords<String, String> records = consumer.poll(Duration.ofMillis(100));

            for (ConsumerRecord<String, String> record : records){
                log.info("[ Key : {}, Value: {} , Partition: {}, Offset:{} ]", record.key(), record.value(),
                        record.partition(), record.offset());
            }
        }
    }

    public static void main(String[] args) {
        new ConsumerDemo().pollForNewData();
    }

}
