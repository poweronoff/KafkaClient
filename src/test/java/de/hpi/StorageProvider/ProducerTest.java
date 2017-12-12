package de.hpi.StorageProvider;

import lombok.AccessLevel;
import lombok.Getter;
import lombok.Setter;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.*;

class ProducerTest {
    @Getter @Setter(AccessLevel.PRIVATE) private static Producer producer;

    @BeforeAll
    static void setup(){
        setProducer(new Producer());
    }

    @Test
    void sendToKafka(){
        producer.sendToKafka("test", "data");
    }
}