package com.github.somendra87.tutorial1.producer;

import org.apache.kafka.clients.producer.*;
import org.apache.kafka.common.serialization.StringSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Properties;
import java.util.concurrent.ExecutionException;

import static com.github.somendra87.KafkaConstants.LOCALHOST_9092;
import static com.github.somendra87.KafkaConstants.VALUE_SERIALIZER;
import static org.apache.kafka.clients.producer.ProducerConfig.BOOTSTRAP_SERVERS_CONFIG;

/**
 * @author somendraprakash created on 16/12/21
 */
public class ProducerDemoWithKey
{
    private static final Logger logger = LoggerFactory.getLogger(ProducerDemoWithKey.class);

    public static void main(String[] args) throws ExecutionException, InterruptedException {
        // create producer properties
        Properties properties = new Properties();
        properties.setProperty(BOOTSTRAP_SERVERS_CONFIG, LOCALHOST_9092);
        properties.setProperty(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        properties.setProperty(VALUE_SERIALIZER, StringSerializer.class.getName());

        //create producer
        KafkaProducer<String, String> producer = new KafkaProducer<>(properties);

        for (int i = 0; i < 10; i++) {

            String topic = "first_topic";
            String value = "hello world" + i;
            String key = "id_" + i;

            // create a producer record
            ProducerRecord<String, String> producerRecord =
                    new ProducerRecord<String, String>(topic, key, value);

            logger.info("key: {}", key);
            //same key goes to same partition
            //send data -- this is asynchronous
            producer.send(producerRecord, (metadata, exception) -> {
                // executes every time a record is successfully sent or an exception occurs
                if (exception == null) {
                    //the record was successfully sent
                    logger.info(
                            "Received Metadata \n"
                                    + "Topic     : " + metadata.topic() + "\n"
                                    + "Partition : " + metadata.partition() + "\n"
                                    + "Offset    : " + metadata.offset() + "\n"
                                    + "TimeStamp : " + metadata.timestamp() + "\n"
                    );
                } else {
                    logger.error("Error while processing ", exception);
                }
            }).get();//forcing send to be synchronous --> bad practice --> never do this in prod

        }

        // flush data
        producer.flush();

        //close producer
        producer.close();
    }
}
