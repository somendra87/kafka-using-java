package com.github.somendra87.tutorial1.producer;

import org.apache.kafka.clients.producer.*;
import org.apache.kafka.common.serialization.StringSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Properties;

import static com.github.somendra87.KafkaConstants.LOCALHOST_9092;
import static com.github.somendra87.KafkaConstants.VALUE_SERIALIZER;
import static org.apache.kafka.clients.producer.ProducerConfig.BOOTSTRAP_SERVERS_CONFIG;

/**
 * @author somendraprakash created on 16/12/21
 */
public class ProducerDemoWithCallback
{
    private static final Logger logger = LoggerFactory.getLogger(ProducerDemoWithCallback.class);

    public static void main(String[] args) {
        // create producer properties
        Properties properties = new Properties();
        properties.setProperty(BOOTSTRAP_SERVERS_CONFIG, LOCALHOST_9092);
        properties.setProperty(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        properties.setProperty(VALUE_SERIALIZER, StringSerializer.class.getName());

        //create producer
        KafkaProducer<String, String> producer = new KafkaProducer<String, String>(properties);

        // create a producer record
        ProducerRecord<String, String> producerRecord =
                new ProducerRecord<String, String>("first_topic", "hello world");

        //send data -- this is asynchronous
        producer.send(producerRecord, new Callback()
        {
            @Override
            public void onCompletion(RecordMetadata metadata, Exception exception) {
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
                }else{
                    logger.error("Error while processing ", exception);
                }
            }
        });

        // flush data
        producer.flush();

        //close producer
        producer.close();
    }
}
