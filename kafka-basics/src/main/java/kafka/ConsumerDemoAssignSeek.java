package kafka;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.util.Arrays;
import java.util.Properties;

public class ConsumerDemoAssignSeek {

    public static void main(String[] args) {

        Logger logger = LoggerFactory.getLogger(ConsumerDemoAssignSeek.class);

        Properties properties = new Properties();
        String bootstrapServers = "localhost:9092";
        String topic = "first_topic";

        // set consumer configurations

            properties.setProperty(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
            properties.setProperty(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
            properties.setProperty(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG,StringDeserializer.class.getName());
            properties.setProperty(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG,"earliest");


        // create consumer

        KafkaConsumer<String,String> consumer = new KafkaConsumer<String, String>(properties);

        TopicPartition partition = new TopicPartition(topic, 0);
        Long offsetToRead = 15L;


        // Assign
        consumer.assign(Arrays.asList(partition));

     // Seek
        consumer.seek(partition, offsetToRead);

        // poll for new data

        boolean keepOnReading = true;
        int numberOfMessagesToRead = 5;
        int numberOfMessagesread = 0;

        while(keepOnReading){
           ConsumerRecords<String, String> records =  consumer.poll(Duration.ofMillis(100));

           for(ConsumerRecord<String,String> record:records){
               numberOfMessagesread += 1;
               logger.info("Key: " + record.key() + " Value: " + record.value());
               logger.info("Partition: " + record.partition() + " Offset: " + record.offset());
               if(numberOfMessagesread>=numberOfMessagesToRead){
                   keepOnReading = false;
                   break;
               }
           }
        }
    }
}
