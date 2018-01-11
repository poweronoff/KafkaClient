package de.hpi.StorageProvider;

import org.apache.log4j.BasicConfigurator;

public class run
{   public static void main(String[] args) {
        BasicConfigurator.configure();
        ConfigurableProducer producer = new ConfigurableProducer();
        producer.createTopic("test123", 1 , 1);
        producer.sendToKafka("test123", "JaJa");

}
}
