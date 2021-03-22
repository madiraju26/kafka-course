package kafka;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;

import java.util.Properties;


public class ProducerDemo {



    public static void main(String[] args) {

        String bootstrapServers = "localhost:9092";


        // create produce properties

        Properties properties = new Properties();
        properties.setProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
        properties.setProperty(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        properties.setProperty(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG,StringSerializer.class.getName());

        // create the producer

        KafkaProducer<String,String> producer = new KafkaProducer<String, String>(properties);

        // create the producer record

        ProducerRecord<String,String> record = new ProducerRecord<String, String>("first_topic","Hello World");

        // send data

        producer.send(record);

        producer.flush();
        producer.close();

    }
}
