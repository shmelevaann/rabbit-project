package ru.chiffa.rabbit;

import com.rabbitmq.client.BuiltinExchangeType;
import com.rabbitmq.client.Channel;
import com.rabbitmq.client.ConnectionFactory;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.Scanner;
import java.util.concurrent.TimeoutException;

public class MessageProducer {
    private static final String EXCHANGE_NAME = "article_exchange";

    public static void main(String[] args) throws IOException, TimeoutException {
        ConnectionFactory factory = new ConnectionFactory();
        factory.setHost("localhost");

        try (Channel channel = factory.newConnection().createChannel();
             Scanner scanner = new Scanner(System.in)) {
            channel.exchangeDeclare(EXCHANGE_NAME, BuiltinExchangeType.TOPIC);
            while (true) {
                String input = scanner.nextLine();

                if (input.equals("exit")) {
                    break;
                }

                if (input.matches("\\w+\\s.*")) {
                    int separatorIndex = input.indexOf(" ");
                    String key = input.substring(0, separatorIndex);
                    String message = input.substring(separatorIndex + 1);

                    channel.basicPublish(EXCHANGE_NAME, key, null, message.getBytes(StandardCharsets.UTF_8));
                    System.out.println("Sent: " + message);
                } else {
                    System.out.println("Enter a message like \'KEY MESSAGE\'");
                }
            }
        }
    }
}
