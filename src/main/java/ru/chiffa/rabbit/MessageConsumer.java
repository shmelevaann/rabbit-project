package ru.chiffa.rabbit;

import com.rabbitmq.client.BuiltinExchangeType;
import com.rabbitmq.client.Channel;
import com.rabbitmq.client.ConnectionFactory;
import com.rabbitmq.client.DeliverCallback;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.Scanner;
import java.util.concurrent.TimeoutException;

public class MessageConsumer {
    private static final String EXCHANGE_NAME = "article_exchange";

    public static void main(String[] args) throws IOException, TimeoutException {
        ConnectionFactory factory = new ConnectionFactory();
        factory.setHost("localhost");
        try (Channel channel = factory.newConnection().createChannel();
             Scanner scanner = new Scanner(System.in)) {
            channel.exchangeDeclare(EXCHANGE_NAME, BuiltinExchangeType.TOPIC);
            String queueName = channel.queueDeclare().getQueue();

            channel.basicConsume(queueName, true, deliverCallBack(), consumerTag -> {
            });

            while (true) {
                String input = scanner.nextLine();

                if (input.equals("exit")) {
                    break;
                }

                if (input.matches("\\w+\\s\\w+")) {
                    int serparatorIndex = input.indexOf(" ");
                    String command = input.substring(0, serparatorIndex).toLowerCase();
                    String key = input.substring(serparatorIndex + 1);

                    switch (command) {
                        case "set_topic":
                            channel.queueBind(queueName, EXCHANGE_NAME, key);
                            System.out.println("Subscribed to the topic '" + key + "'");
                            break;
                        case "delete_topic":
                            channel.queueUnbind(queueName, EXCHANGE_NAME, key);
                            System.out.println("Unsubscribed to the topic '" + key + "'");
                            break;
                        default:
                            printInstructions();
                    }
                } else {
                    printInstructions();
                }
            }
        }
    }

    private static void printInstructions() {
        System.out.println("To subscribe topic send 'set_topic topic'");
        System.out.println("To unsubscribe topic send 'delete_topic topic'");
    }

    private static DeliverCallback deliverCallBack() {
        return (consumerTag, delivery) -> {
            String message = new String(delivery.getBody(), StandardCharsets.UTF_8);
            System.out.println("Received: '" + delivery.getEnvelope().getRoutingKey() + "':" + message);
        };
    }
}
