package de.hpi.StorageProvider;

import lombok.AccessLevel;
import lombok.Getter;
import lombok.Setter;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.runner.RunWith;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.support.SendResult;
import org.springframework.test.context.junit4.SpringRunner;

import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.util.concurrent.ListenableFuture;
import org.springframework.util.concurrent.ListenableFutureCallback;

import java.util.concurrent.TimeUnit;
import static org.junit.Assert.assertTrue;

@RunWith(SpringRunner.class)
@SpringBootTest
class ProducerTest {
    @Autowired @Getter @Setter(AccessLevel.PRIVATE) private static Producer producer;
    @Autowired @Getter @Setter(AccessLevel.PRIVATE) private static Listener listener;

    @Autowired @Getter @Setter(AccessLevel.PRIVATE) private static KafkaTemplate<String, String> kafkaTemplate;

    @BeforeAll
    static void setup(){
        setProducer( new Producer());
        setListener(new Listener());

        getProducer().createTemplate();
    }


    @Test
    void createTopic() {
        getProducer().createTopic("test",1,1);
    }
    @Test
    void sendToKafka(){
        getProducer().sendToKafka("test", "data");
    }

    @Test
    void contextLoads() throws InterruptedException {
        KafkaProducerConfig config = new KafkaProducerConfig();
        KafkaTemplate<String, String> template = config.kafkaTemplate();
        System.out.println(template);
        ListenableFuture<SendResult<String, String>> future = template.send("test", "ABC");
        future.addCallback(new ListenableFutureCallback<SendResult<String, String>>() {
            @Override
            public void onSuccess(SendResult<String, String> result) {
                System.out.println("success");
            }

            @Override
            public void onFailure(Throwable ex) {
                System.out.println("failed");
            }
        });
        //System.out.println(Thread.currentThread().getId());
        assertTrue(listener.countDownLatch1.await(60, TimeUnit.SECONDS));

    }
}