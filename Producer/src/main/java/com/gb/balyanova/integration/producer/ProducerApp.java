package com.gb.balyanova.integration.producer;

import com.rabbitmq.client.BuiltinExchangeType;
import com.rabbitmq.client.Channel;
import com.rabbitmq.client.Connection;
import com.rabbitmq.client.ConnectionFactory;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.concurrent.TimeoutException;

public class ProducerApp {
    private static final String EXCHANGE_NAME = "it_blog";

    public static void main(String[] args) {
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
                    String topic = msg[0];
                    String message = topic_message.substring(topic.length() + 1);
                    String routingKey = "it_blog." + topic;

                    channel.basicPublish(EXCHANGE_NAME, routingKey, null, message.getBytes("UTF-8"));
                    System.out.println(" [x] Sent '" + routingKey + "':'" + message + "'");

                } if (topic_message.equals("exit")) {
                    break;
                }
            }
        } catch (IOException | TimeoutException e) {
            e.printStackTrace();
        }
    }
}