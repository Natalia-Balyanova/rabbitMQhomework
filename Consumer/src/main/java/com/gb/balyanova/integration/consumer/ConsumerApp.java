package com.gb.balyanova.integration.consumer;

import com.rabbitmq.client.*;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.TimeoutException;

public class ConsumerApp {
    private static final String EXCHANGE_NAME = "it_blog";

    public static void main(String[] args)  {
        Map<String, String> queueMap = new HashMap<>();

        ConnectionFactory factory = new ConnectionFactory();
        factory.setHost("localhost");
        try (Connection connection = factory.newConnection();
            Channel channel = connection.createChannel()) {
            channel.exchangeDeclare(EXCHANGE_NAME, BuiltinExchangeType.TOPIC);

            BufferedReader reader = new BufferedReader(new InputStreamReader(System.in));
            while (true) {
                String topic_message = reader.readLine();

                if (topic_message.contains(" ")) {
                    String[] msg = topic_message.split(" ");
                    String command = msg[0];
                    if (command.equals("set_topic")) {
                        String queueName = channel.queueDeclare().getQueue();
                        System.out.println("QUEUE NAME: " + queueName);

                        String routingKey = "it_blog." + msg[1];
                        channel.queueBind(queueName, EXCHANGE_NAME, routingKey);
                        queueMap.put(routingKey, queueName);
                        System.out.println(" [*] Waiting for messages for (" + routingKey + "):");

                        DeliverCallback deliverCallback = (consumerTag, delivery) -> {
                            String message = new String(delivery.getBody(), "UTF-8");
                            System.out.println(" [x] Received '" + delivery.getEnvelope().getRoutingKey() + "':'" + message + "'");

                        };
                        channel.basicConsume(queueName, true, deliverCallback, consumerTag -> {});

                    } else if (command.equals("unsubscribe")) { //возможность подписчикам подписываться и отписываться от статей по темам
                        String routingKey = "it_blog." + msg[1];
                        channel.queueUnbind(queueMap.get(routingKey), EXCHANGE_NAME, routingKey);
                        queueMap.remove(routingKey);
                        System.out.println(routingKey + ": unsubscribed");
                    }
                } if (topic_message.equals("exit")) {
                    break;
                }
            }
        } catch (IOException | TimeoutException e) {
            e.printStackTrace();
        }
    }
}
