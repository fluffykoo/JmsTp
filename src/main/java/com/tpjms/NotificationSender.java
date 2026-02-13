package com.tpjms;

import javax.jms.*;
import org.apache.activemq.ActiveMQConnectionFactory;

/**
 * Ce service traite les notifications en attente,envoie des emails, push notifications, SMS, etc.*/
public class NotificationSender {

    public static void main(String[] args) throws Exception {

        ConnectionFactory factory =
                new ActiveMQConnectionFactory("tcp://localhost:61616");

        Connection connection = factory.createConnection();
        Session session =
                connection.createSession(false, Session.AUTO_ACKNOWLEDGE);

        Queue queue = session.createQueue("NOTIFICATION_QUEUE");

        MessageConsumer consumer = session.createConsumer(queue);

        connection.start();

        System.out.println("NotificationSender actif - Traitement des notifications...");


        while (true) {
            Message msg = consumer.receive();
            if (msg instanceof TextMessage) {
                String notification = ((TextMessage) msg).getText();

                String[] parts = notification.split("\\|");
                String recipient = parts[1].split(":")[1];
                String message = parts[2].split(":")[1];

                // Simuler l'envoi (email, push, SMS)
                System.out.println("Envoi notification à " + recipient + " : \"" + message + "\"");

                Thread.sleep(500); // Simuler le temps de traitement
            }
        }
    }
}