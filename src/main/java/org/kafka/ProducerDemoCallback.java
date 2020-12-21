package org.kafka;

import org.apache.kafka.clients.producer.*;
import org.apache.kafka.common.serialization.StringSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Properties;

/**
 * Hello world!
 *
 */
public class ProducerDemoCallback
{
   static Logger logger = LoggerFactory.getLogger(ProducerDemoCallback.class);

    public static void main( String[] args )
    {
        // create producer properties

        Properties properties = new Properties();
        properties.setProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG,"127.0.0.1:9092");
        properties.setProperty(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        properties.setProperty(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG,StringSerializer.class.getName());

        //create producer
        KafkaProducer<String,String> producer = new KafkaProducer<String, String>(properties);

        //create producer record
        for (int i=0;i<10;i++) {
            ProducerRecord<String, String> record = new ProducerRecord<String, String>("first_topic", "hello mate "+ Integer.toString(i) );

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
            });

        }
        producer.flush();

        producer.close();
    }
}
