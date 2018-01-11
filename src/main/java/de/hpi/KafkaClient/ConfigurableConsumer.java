package de.hpi.KafkaClient;

import java.util.Properties;
import java.util.Arrays;
import java.util.function.Consumer;

import lombok.AccessLevel;
import lombok.Getter;
import lombok.Setter;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.common.serialization.StringDeserializer;

public class ConfigurableConsumer {
    @Getter(AccessLevel.PRIVATE) @Setter(AccessLevel.PRIVATE) private static KafkaConsumer<String, String> consumer;
    public ConfigurableConsumer(String groupId, Consumer<String> recipient, String topic) {
        Properties props = new Properties();
        props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, "ts1552.byod.hpi.de:9092");
        props.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, "true");
        props.put(ConsumerConfig.AUTO_COMMIT_INTERVAL_MS_CONFIG, "1000");
        props.put(ConsumerConfig.SESSION_TIMEOUT_MS_CONFIG, "30000");
        props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
        props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
        props.put("group.id", groupId);

        consumer = new KafkaConsumer<>(props);
        runConsumer(topic, recipient);
    }

    public static void runConsumer(String topic,Consumer<String> recipient) {
        consumer.subscribe(Arrays.asList(topic));
        while (true) {
            ConsumerRecords<String, String> records = consumer.poll(100);
            for (ConsumerRecord<String, String> record : records)
                recipient.accept(record.value());
                //System.out.printf("offset = %d, key = %s, value = %s\n",
                  //      record.offset(), record.key(), record.value());
        }
    }

    /*public static void main(String[] args) throws Exception {


    }*/

}