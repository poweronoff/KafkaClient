package de.hpi.KafkaClient;

import java.util.function.Consumer;

public class runConsumer
{

    public static void main(String[] args) {
        Consumer<String> consumer1 = (arg) -> {
            System.out.println(arg);
        };
        ConfigurableConsumer consumer = new ConfigurableConsumer("15", consumer1, "test1234");



}
}

