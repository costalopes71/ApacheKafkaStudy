package com.costalopes.apachekafkastudy.consumer;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;

import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.Properties;

public class KafkaConsumerSubscribeApp {

    public static void main(String[] args) {

        // criar as propriedades requirdas e opcionais para o consumidor
        Properties props = new Properties();
        props.put("bootstrap.servers", "localhost:9092, localhost:9093");
        props.put("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        props.put("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        props.put("group.id", "test");
        // para mais opcoes https://kafka.apache.org/documentation.html#consumerconfigs

        KafkaConsumer myConsumer = new KafkaConsumer(props);

        List<String> topics = new ArrayList<>();
        topics.add("my-topic");
        topics.add("my-other-topic");

        myConsumer.subscribe(topics);

        try {

            while (true) {

                ConsumerRecords<String, String> consumerRecords = myConsumer.poll(Duration.ofMillis(10));
                for (ConsumerRecord<String, String> record : consumerRecords) {

                    //processar cada registro
                    System.out.println(
                            String.format("topic: %s, Partition: %d, Offset: %d, Key: %s, Value: %s",
                            record.topic(), record.partition(), record.offset(), record.key(), record.value()));

                }

            }

        } catch (Exception e) {

            System.out.println(e.getMessage());

        } finally {

            myConsumer.close();

        }

    }

}
