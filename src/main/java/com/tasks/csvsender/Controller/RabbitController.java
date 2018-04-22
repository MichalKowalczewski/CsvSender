package com.tasks.csvsender.Controller;

import com.rabbitmq.client.Channel;
import com.rabbitmq.client.Connection;
import com.rabbitmq.client.ConnectionFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.PropertySource;
import org.springframework.stereotype.Controller;

import java.io.IOException;
import java.util.concurrent.TimeoutException;

@Controller
@PropertySource("classpath:application.properties")
public class RabbitController {
    private ConnectionFactory factory;
    private Connection connection;
    private Channel channel;

    @Value("${rabbit.exchange.name}")
    String exchange;

    @Value("${rabbit.queue}")
    String queue;

    @Value("${rabbit.exchange.type}")
    String type;

    @Value("${rabbit.host}")
    String host;

    public void setFactory(){
        factory = new ConnectionFactory();
        factory.setHost(host);
    }

    public ConnectionFactory getFactory(){
        return factory;
    }

    public void setConnection() {
        try {
            connection = getFactory().newConnection();
        } catch (IOException e) {
            e.printStackTrace();
        } catch (TimeoutException e) {
            e.printStackTrace();
        }
    }

    public Connection getConnection(){
        return connection;
    }

    public void setChannel() {
        setFactory();
        setConnection();

        try {
            channel = getConnection().createChannel();
            channel.exchangeDeclare(exchange, type);
        }
        catch (IOException e) {
            e.printStackTrace();
        }
    }

    public Channel getChannel() {
        return channel;
    }

    public void closeChannel(){
        try {
            channel.close();
        } catch (IOException e) {
            e.printStackTrace();
        } catch (TimeoutException e) {
            e.printStackTrace();
        }
    }

    public void closeConnection(){
        try {
            connection.close();
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    public void rabbitSend(String json, String filename) {
        Channel channel = getChannel();
        try {
            channel.basicPublish(exchange, filename, null, json.getBytes());
            System.out.println(json);
        } catch (IOException e) {
            e.printStackTrace();
        }

    }


}
