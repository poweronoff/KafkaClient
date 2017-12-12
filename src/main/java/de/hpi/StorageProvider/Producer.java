package de.hpi.StorageProvider;


import lombok.AccessLevel;
import lombok.Getter;
import lombok.Setter;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.context.annotation.Bean;
import org.springframework.kafka.core.DefaultKafkaProducerFactory;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.core.ProducerFactory;
import org.springframework.kafka.support.SendResult;
import org.springframework.util.concurrent.ListenableFuture;
import org.springframework.util.concurrent.ListenableFutureCallback;

import java.util.HashMap;
import java.util.Map;

@SpringBootApplication
public class Producer{

    @Getter @Setter(AccessLevel.PRIVATE) KafkaTemplate<Integer, String> template;

    // initialization
    public Producer(){
        setTemplate(kafkaTemplate());
    }

    @Bean
    public ProducerFactory<Integer, String> producerFactory() {
        return new DefaultKafkaProducerFactory<>(producerConfigs());
    }

    @Bean
    public Map<String, Object> producerConfigs() {
        Map<String, Object> props = new HashMap<>();
        props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "ts1552.byod.hpi.de:9092");
        props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class);
        props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class);
        // See https://kafka.apache.org/documentation/#producerconfigs for more properties
        return props;
    }

    @Bean
    public KafkaTemplate<Integer, String> kafkaTemplate() {
        return new KafkaTemplate<>(producerFactory());
    }

    // convenience
    public void sendToKafka(String topic, final String data) {
        final ProducerRecord<String, String> record = createRecord(topic, data);

        ListenableFuture<SendResult<Integer, String>> future = getTemplate().send(topic, data);
        future.addCallback(new ListenableFutureCallback<SendResult<Integer, String>>() {

            @Override
            public void onSuccess(SendResult<Integer, String> result) {
                handleSuccess(data);
            }

            @Override
            public void onFailure(Throwable ex) {
                handleFailure(data, record, ex);
            }

        });
    }

    // actions
    private ProducerRecord<String, String> createRecord(String topic, String data) {
        return new ProducerRecord<>(topic, data);
    }

    private void handleSuccess(String data) {
        System.out.println("Wrote data to queue.\nData: " + data);
    }

    private void handleFailure(String data, ProducerRecord record, Throwable ex) {
        System.out.println("Failed to write data to queue.\nData: " + data + "\nTopic: " + record.topic());
        ex.printStackTrace();
    }
}