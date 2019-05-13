package com.costalopes.apachekafkastudy.consumer.group;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;

import java.util.Properties;

public class KafkaGroupProducerApp {

    public static void main(String[] args) {

        Properties props = new Properties();
        props.put("bootstrap.servers", "localhost:9092");
        props.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        props.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");

        KafkaProducer<String, String> myProducer = new KafkaProducer<>(props);

        try {

            int counter = 0;
            while (counter < 100) {

                myProducer.send(new ProducerRecord<String, String>("my-big-topic", "abcdefghijklmnopqrstuvxz"));
                counter++;

            }

        } catch (Exception e) {

            e.printStackTrace();

        } finally {

            myProducer.close();

        }


    }

}
