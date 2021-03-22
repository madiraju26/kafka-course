package kafka;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.errors.WakeupException;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.util.Arrays;
import java.util.Properties;
import java.util.concurrent.CountDownLatch;

public class ConsumerDemoWithThreads {

    public static void main(String[] args) {

    new ConsumerDemoWithThreads().run();

    }

    private ConsumerDemoWithThreads(){

    }

    private void run(){

        Logger logger = LoggerFactory.getLogger(ConsumerRunnable.class);
        String bootstrapServers = "localhost:9092";
        String groupId = "my-last-application";
        String topic = "first_topic";

        // latch for dealing with multiple threads
        CountDownLatch latch = new CountDownLatch(1);


        logger.info("Creating consumer runnable");
        Runnable myConsumerRunnable = new ConsumerRunnable(topic,bootstrapServers,groupId,latch);

        // starting the thread
        Thread myThread = new Thread(myConsumerRunnable);
        myThread.start();

        //add a shutdown hook

        Runtime.getRuntime().addShutdownHook(new Thread(()->{
        logger.info("caught shutdown hook");
        ((ConsumerRunnable) myConsumerRunnable).shutdown();
            try {
                latch.await();
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
            logger.info("Application got exited");
        }));


        try {
            latch.await();
        } catch (InterruptedException e) {
            logger.error("Application is interupted ", e);
        } finally {
            logger.info("Application is closing");
        }
    }

    public class ConsumerRunnable implements Runnable{

        private CountDownLatch latch;
        private KafkaConsumer<String,String> consumer;
        private Logger logger = LoggerFactory.getLogger(ConsumerRunnable.class);

        public ConsumerRunnable(String topic,
                                String bootstrapServers, String groupId
                               , CountDownLatch latch){


            this.latch =latch;

            // set consumer configurations
            Properties properties = new Properties();

            properties.setProperty(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
            properties.setProperty(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
            properties.setProperty(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG,StringDeserializer.class.getName());
            properties.setProperty(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG,"earliest");
            properties.setProperty(ConsumerConfig.GROUP_ID_CONFIG,groupId);


            // create consumer

           consumer = new KafkaConsumer<String, String>(properties);

            // subscribe consumer to topics
            consumer.subscribe(Arrays.asList(topic));
        }

        @Override
        public void run() {
            // poll for new data

            try {

                while (true) {
                    ConsumerRecords<String, String> records = consumer.poll(Duration.ofMillis(100));

                    for (ConsumerRecord<String, String> record : records) {
                        logger.info("Key: " + record.key() + " Value: " + record.value());
                        logger.info("Partition: " + record.partition() + " Offset: " + record.offset());
                    }
                }
            } catch (WakeupException e){
                logger.info("Received  shutdown signal");
            } finally {
                consumer.close();

                // tell our main code we are done with consumer
                latch.countDown();
            }
        }

        public void shutdown(){

            consumer.wakeup();

        }
    }
}
