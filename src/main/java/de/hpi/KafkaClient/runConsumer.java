package de.hpi.KafkaClient;

import java.util.function.Consumer;
// TODO - java classes must have a capital letter as a fist
// TODO - use slf4j for logging, do not use System.out.println
// TODO - format the braces
public class runConsumer
{

    public static void main(String[] args) {
        Consumer<String> consumer1 = (arg) -> {
            System.out.println(arg);
        };
        ConfigurableConsumer consumer = new ConfigurableConsumer("15", consumer1, "test1234");



}
}

