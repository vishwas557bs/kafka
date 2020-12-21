package org.kafka;

import org.apache.kafka.clients.producer.*;
import org.apache.kafka.common.serialization.StringSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Properties;
import java.util.concurrent.ExecutionException;

/**
 * Hello world!
 *
 */
public class ProducerDemoKeys
{
   static Logger logger = LoggerFactory.getLogger(ProducerDemoKeys.class);

    public static void main(String[] args ) throws ExecutionException, InterruptedException {
        // create producer properties

        Properties properties = new Properties();
        properties.setProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG,"127.0.0.1:9092");
        properties.setProperty(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        properties.setProperty(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG,StringSerializer.class.getName());

        //create producer
        KafkaProducer<String,String> producer = new KafkaProducer<String, String>(properties);

        //create producer record
        for (int i=0;i<10;i++) {
            String topic = "first_topic";
            String value = "hello world " + Integer.toString(i);
            String key = "id_" + Integer.toString(i);

            ProducerRecord<String, String> record = new ProducerRecord<String, String>( topic,value);

            logger.info("key : "+key);

            //send data
            producer.send(record, new Callback() {
                @Override
                public void onCompletion(RecordMetadata recordMetadata, Exception e) {
                    logger.info("Received metadata : \n"+
                            "Topic: "+ recordMetadata.topic() + "\n" +
                            "Offset: "+ recordMetadata.offset() + "\n"+
                            "time; "+ recordMetadata.timestamp() + "\n"+
                            "partition: "+ recordMetadata.partition()
                    );
                }
            }).get();

        }
        producer.flush();

        producer.close();
    }
}
