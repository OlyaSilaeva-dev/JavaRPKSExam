package org.exam.task28;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;
import org.springframework.boot.CommandLineRunner;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;

import java.util.Properties;

@SpringBootApplication
public class Task28Application implements CommandLineRunner {

    public static void main(String[] args) {

        SpringApplication.run(Task28Application.class, args);
    }

    @Override
    public void run(String... args) {
        System.out.println("Отправка тестового Kafka-сообщения...");

        Properties props = new Properties();
        props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
        props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class);
        props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class);

        try (KafkaProducer<String, String> producer = new KafkaProducer<>(props)) {
            ProducerRecord<String, String> record =
                    new ProducerRecord<>("my-topic", "test", "SomeKafkaMessage");
            producer.send(record);
            System.out.println("Сообщение отправлено в Kafka");
        } catch (Exception e) {
            System.err.println("Ошибка при отправке сообщения: " + e.getMessage());
        }
    }
}
