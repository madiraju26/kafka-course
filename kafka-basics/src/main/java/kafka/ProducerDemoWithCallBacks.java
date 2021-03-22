package kafka;

import org.apache.kafka.clients.producer.*;
import org.apache.kafka.common.serialization.StringSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Properties;


public class ProducerDemoWithCallBacks {


    public static void main(String[] args) {

        final Logger logger = LoggerFactory.getLogger(ProducerDemoWithCallBacks.class);

        String bootstrapServers = "localhost:9092";


        // create produce properties

        Properties properties = new Properties();
        properties.setProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
        properties.setProperty(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        properties.setProperty(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());

        // create the producer

        KafkaProducer<String, String> producer = new KafkaProducer<String, String>(properties);

        // create the producer record

        for (int i = 0; i < 10; i++) {

            ProducerRecord<String, String> record =
                    new ProducerRecord<String, String>("first_topic", "Hello World" + Integer.toString(i));

            // send data

            producer.send(record, new Callback() {
                public void onCompletion(RecordMetadata recordMetadata, Exception e) {
                    if (e == null) {
                        logger.info("Received new metadata." + "\n" +
                                "Topic:" + recordMetadata.topic() + "\n" +
                                "Partition:" + recordMetadata.partition() + "\n" +
                                "Offset: " + recordMetadata.offset() + "\n" +
                                "Timestamp " + recordMetadata.timestamp());
                    } else {
                        logger.error("Error While Producing" + e);
                    }
                }
            });

        }

        producer.flush();
        producer.close();
    }
}
