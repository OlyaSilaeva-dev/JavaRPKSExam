package org.exam.task28;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.springframework.data.redis.core.RedisTemplate;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;

import java.time.Duration;
import java.util.Collections;
import java.util.Properties;
import java.util.UUID;

@RestController
@RequestMapping("/task")
public class TaskController {

    private final RedisTemplate<String, String> redisTemplate;

    public TaskController(RedisTemplate<String, String> redisTemplate) {
        this.redisTemplate = redisTemplate;
    }

    @GetMapping("/consume")
    public ResponseEntity<String> consumeFromKafka(
            @RequestParam String bootstrapServers,
            @RequestParam String topic) {

        Properties props = new Properties();
        props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
        props.put(ConsumerConfig.GROUP_ID_CONFIG, "task-consumer-group-" + UUID.randomUUID());
        props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
        props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
        props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");

        try (KafkaConsumer<String, String> consumer = new KafkaConsumer<>(props)) {
            consumer.subscribe(Collections.singletonList(topic));

            ConsumerRecords<String, String> records;
            long start = System.currentTimeMillis();
            String firstKey = null;

            while (System.currentTimeMillis() - start < 5000) {
                records = consumer.poll(Duration.ofMillis(500));
                for (ConsumerRecord<String, String> record : records) {
                    firstKey = record.key();
                    if (firstKey != null) {
                        break;
                    }
                }
                if (firstKey != null) break;
            }

            if (firstKey == null) {
                return ResponseEntity.status(HttpStatus.NOT_FOUND)
                        .body("No messages with key found in topic.");
            }

            String redisValue = redisTemplate.opsForValue().get(firstKey);
            if (redisValue == null) {
                return ResponseEntity.status(HttpStatus.NOT_FOUND)
                        .body("No value found in Redis for key: " + firstKey);
            }

            StringBuilder sb = new StringBuilder();
            for (int i = 0; i < redisValue.length(); i += 2) {
                sb.append(redisValue.charAt(i));
            }

            return ResponseEntity.ok(sb.toString());
        } catch (Exception e) {
            return ResponseEntity.status(HttpStatus.INTERNAL_SERVER_ERROR)
                    .body("Error: " + e.getMessage());
        }
    }
}
