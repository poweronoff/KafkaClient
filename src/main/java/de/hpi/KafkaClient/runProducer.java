package de.hpi.KafkaClient;

import org.apache.log4j.BasicConfigurator;

public class runProducer
{   public static void main(String[] args) {
        //BasicConfigurator.configure();
        ConfigurableProducer producer = new ConfigurableProducer("15");
        producer.createTopic("test1234", 4 , 1);
        for (int i =0; i<100; i++){
                producer.sendToKafka("test1234", Integer.toString(i));
        }
        producer.closeProducer();
}
}
