package com.costalopes.apachekafkastudy.producer;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;

import java.util.Properties;

public class KafkaProducerApp {

    public static void main(String[] args) {

        Properties props = new Properties();
        props.put("bootstrap.servers", "localhost:9092, localhost:9093");
        props.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        props.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");

        //
        // instanciando o producer
        //

        // exemplo de record com propriedades opcionais setadas no construtor
        @SuppressWarnings("unused")
        ProducerRecord optionalProperties = new ProducerRecord("my_topic", 1, 1234545454425L, "Course-001", "My Message Opcional");

        //
        // instanciando records em um for e mandando para o topico
        //
        try (KafkaProducer<String, String> myProducer = new KafkaProducer<>(props)) {

            for (int i = 0; i < 150; i++) {

                myProducer.send(new ProducerRecord("my_topic", Integer.toString(i), "My Message " + Integer.toString(i)));

            }


        } catch (Exception e) {

            e.printStackTrace();

        }

    }

}
