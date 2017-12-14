package de.hpi.StorageProvider;


import kafka.admin.RackAwareMode;
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
import kafka.admin.AdminUtils;
import kafka.utils.ZKStringSerializer$;
import kafka.utils.ZkUtils;
import org.I0Itec.zkclient.ZkClient;
import org.I0Itec.zkclient.ZkConnection;

import java.util.HashMap;
import java.util.Map;
import java.util.Properties;

@SpringBootApplication
public class Producer{

    @Getter @Setter(AccessLevel.PRIVATE) KafkaTemplate<String, String> template;
/*
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
        return props;
    }
*
    @Bean
    public KafkaTemplate<Integer, String> kafkaTemplate() {
        return new KafkaTemplate<>(producerFactory());
    } */

    // convenience

    public void createTemplate(){
        KafkaProducerConfig config = new KafkaProducerConfig();
        setTemplate(config.kafkaTemplate());
    }

    public void createTopic(String topic, int partitions, int replication) {
        ZkClient zkClient = null;
        String zookeeperHosts = "ts1552.byod.hpi.de:2181";
        int sessionTimeOutInMs = 15 * 1000; // 15 secs
        int connectionTimeOutInMs = 10 * 1000; // 10 secs

        try {
            zkClient = new ZkClient(zookeeperHosts, sessionTimeOutInMs, connectionTimeOutInMs, ZKStringSerializer$.MODULE$);
            ZkUtils zkUtils = new ZkUtils(zkClient, new ZkConnection(zookeeperHosts), false);

            Properties topicConfiguration = new Properties();

            AdminUtils.createTopic(zkUtils, topic, partitions, replication, topicConfiguration, RackAwareMode.Enforced$.MODULE$);

        }
         catch (Exception ex) {
            ex.printStackTrace();
        } finally {
            if (zkClient != null) {
                zkClient.close();
            }
        }
    }

    public void sendToKafka(String topic, final String data) {
        final ProducerRecord<String, String> record = createRecord(topic, data);

        ListenableFuture<SendResult<String, String>> future = getTemplate().send(topic, data);
        future.addCallback(new ListenableFutureCallback<SendResult<String, String>>() {

            @Override
            public void onSuccess(SendResult<String, String> result) {
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