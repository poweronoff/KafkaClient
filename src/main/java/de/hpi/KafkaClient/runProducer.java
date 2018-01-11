package de.hpi.KafkaClient;

import org.apache.log4j.BasicConfigurator;
// TODO - java classes must have a capital letter as a fist
// TODO - format the braces
public class runProducer
{   public static void main(String[] args) {
        //BasicConfigurator.configure(); // do not comment out the code, delete it
        ConfigurableProducer producer = new ConfigurableProducer("15");
        producer.createTopic("test1234", 4 , 1);
        for (int i =0; i<100; i++){
                producer.sendToKafka("test1234", Integer.toString(i));
        }
        producer.closeProducer();
}
}
