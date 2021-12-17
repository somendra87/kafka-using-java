package com.github.somendra87.tutorial2;

import com.google.common.collect.Lists;
import com.twitter.hbc.ClientBuilder;
import com.twitter.hbc.core.Client;
import com.twitter.hbc.core.Constants;
import com.twitter.hbc.core.endpoint.StatusesFilterEndpoint;
import com.twitter.hbc.core.processor.StringDelimitedProcessor;
import com.twitter.hbc.httpclient.auth.Authentication;
import com.twitter.hbc.httpclient.auth.OAuth1;
import org.apache.kafka.clients.producer.*;
import org.apache.kafka.common.serialization.StringSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;
import java.util.Properties;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;

import static com.github.somendra87.KafkaConstants.LOCALHOST_9092;
import static com.github.somendra87.KafkaConstants.VALUE_SERIALIZER;
import static org.apache.kafka.clients.producer.ProducerConfig.BOOTSTRAP_SERVERS_CONFIG;

/**
 * @author somendraprakash created on 16/12/21
 */
public class TwitterProducer
{
    private static final Logger log = LoggerFactory.getLogger(TwitterProducer.class.getName());
    private static final String consumerKey = "3k4hNTgsnWdDATdUSV9pMTmtr";
    private static final String consumerSecret = "493djhvqRNNokirgmFAWTMKc4YBumtqqX6F7lKJrNiOkmpyKYx";
    private static final String token = "1471419552617533440-WKSOzYwE8LXLduoGRov7SIXkBY5wxp";
    private static final String secret = "4q9pn7CKjxMIEsrJWHkZvoTThbG1QKZpia5QDZNb8LI1a";

    List<String> terms = Lists.newArrayList( "bitcoin", "politics", "amazon");

    public static void main(String[] args) {
        new TwitterProducer().run();
    }

    public void run() {
        /*
         *  Set up your blocking queues: Be sure to size these properly based on expected TPS of your stream
         */
        BlockingQueue<String> msgQueue = new LinkedBlockingQueue<String>(1000);

        // CREATE A TWITTER CLIENT
        Client client = createTwitterClient(msgQueue);

        // attempts to establish connection
        client.connect();

        // CREATE A KAFKA PRODUCER
        KafkaProducer<String, String> producer = createKafkaProducer();

        // shutdown hook
        Runtime.getRuntime().addShutdownHook(
                new Thread(() -> {
                    log.info("stopping Application...");
                    log.info("shutting down client from twitter...");
                    client.stop();
                    log.info("closing producer...");
                    producer.close();
                    log.info("done..");
                })
        );

        // LOOP TO SEND TWEETS TO KAFKA

        // on a different thread, or multiple different threads....
        while (!client.isDone()) {
            String msg = null;
            try {
                msg = msgQueue.poll(5, TimeUnit.SECONDS);
            }
            catch (InterruptedException e) {
                e.printStackTrace();
                client.stop();
            }
            if (msg != null) {
                log.info(msg);
                producer.send(new ProducerRecord<>("twitter_tweets", null, msg), new Callback()
                {
                    @Override
                    public void onCompletion(RecordMetadata metadata, Exception exception) {
                        if (exception != null) {
                            log.error("Something bad happened", exception);
                        }
                    }
                });
            }
        }

        log.info("Exiting Application");
    }

    private KafkaProducer<String, String> createKafkaProducer() {
        // create producer properties
        Properties properties = new Properties();
        properties.setProperty(BOOTSTRAP_SERVERS_CONFIG, LOCALHOST_9092);
        properties.setProperty(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        properties.setProperty(VALUE_SERIALIZER, StringSerializer.class.getName());

        // create sage producer
        properties.setProperty(ProducerConfig.ENABLE_IDEMPOTENCE_CONFIG, "true");
        properties.setProperty(ProducerConfig.ACKS_CONFIG, "all");
        properties.setProperty(ProducerConfig.RETRIES_CONFIG, Integer.toString(Integer.MAX_VALUE));
        properties.setProperty(ProducerConfig.MAX_IN_FLIGHT_REQUESTS_PER_CONNECTION, "5"); //kafka 3.0 >= 1.1 so we
        // can this as 5 , use 1 otherwise

        //high throughput producer (at expense of bit of latency and CPU usage
        properties.setProperty(ProducerConfig.COMPRESSION_TYPE_CONFIG, "snappy");
        properties.setProperty(ProducerConfig.LINGER_MS_CONFIG, "20");
        properties.setProperty(ProducerConfig.BATCH_SIZE_CONFIG, Integer.toString(32 * 1024)); // 32KB batch size

        return new KafkaProducer<>(properties);
    }

    public Client createTwitterClient(BlockingQueue<String> msgQueue) {
        /** Declare the host you want to connect to, the endpoint, and authentication (basic auth or oauth) */
        //Hosts hosebirdHosts = new HttpHosts("https://stream.twitter.com");
        StatusesFilterEndpoint hosebirdEndpoint = new StatusesFilterEndpoint();


        hosebirdEndpoint.trackTerms(terms);

        // These secrets should be read from a config file
        Authentication hosebirdAuth = new OAuth1(consumerKey, consumerSecret, token, secret);

        ClientBuilder builder = new ClientBuilder()
                .name("Hosebird-Client-01") // optional: mainly for the logs
                .hosts(Constants.SITESTREAM_HOST)
                .authentication(hosebirdAuth)
                .endpoint(hosebirdEndpoint)
                .processor(new StringDelimitedProcessor(msgQueue));

        return builder.build();
    }
}
