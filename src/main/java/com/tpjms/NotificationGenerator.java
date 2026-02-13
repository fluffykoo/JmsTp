package com.tpjms;

import java.net.URI;
import javax.jms.*;

import org.apache.activemq.ActiveMQConnectionFactory;
import org.apache.activemq.broker.BrokerFactory;
import org.apache.activemq.broker.BrokerService;

/* Ce service écoute les événements de posts et génère des notifications
 pour les followers de l'utilisateur*/

public class NotificationGenerator implements MessageListener {

    private Session serverSession;
    private MessageProducer queueProducer;

    public static void main(String[] args) throws Exception {
        NotificationGenerator notificationGenerator = new NotificationGenerator();
        notificationGenerator.startTopicListener();
    }

    public void startTopicListener() throws Exception {

        BrokerService broker = BrokerFactory.createBroker(
                new URI("broker:(tcp://localhost:61616)"));
        broker.start();
        System.out.println("Broker ActiveMQ démarré");

        ConnectionFactory connectionFactory =
                new ActiveMQConnectionFactory("tcp://localhost:61616");

        Connection connection = connectionFactory.createConnection();
        connection.setClientID("notificationGeneratorService");
        serverSession = connection.createSession(false, Session.AUTO_ACKNOWLEDGE);

        Topic topic = serverSession.createTopic("POST_EVENTS");

        // Création de la Queue
        javax.jms.Queue queue = serverSession.createQueue("NOTIFICATION_QUEUE");
        queueProducer = serverSession.createProducer(queue);

        MessageConsumer consumer = serverSession.createConsumer(topic);
        consumer.setMessageListener(this);

        connection.start();

        System.out.println("NotificationGenerator actif - Écoute de POST_EVENTS...");

        // Laisse l'application tourner
        Thread.sleep(60000);
    }

    @Override
    public void onMessage(Message message) {
        try {
            TextMessage topicMessage = (TextMessage) message;
            String eventData = topicMessage.getText();

            System.out.println("Événement reçu de PostEventPublisher: " + eventData);

            String[] parts = eventData.split("\\|");
            String action = parts[0];
            String userId = parts[1].split(":")[1];
            String postId = parts[2].split(":")[1];

            // Simuler la récupération des followers (en vrai : requête BDD)
            String[] followers = {"est_rvp", "oumou_cmr", "william_jkp"};

            // Générer une notification pour chaque follower
            for (String followerId : followers) {
                String notificationMessage = String.format(
                        "NOTIFICATION|to:%s|message:%s a créé un nouveau post|postId:%s",
                        followerId, userId, postId
                );

                TextMessage queueMessage = serverSession.createTextMessage(notificationMessage);
                queueProducer.send(queueMessage);

                System.out.println("Notification générée pour " + followerId);
            }

            System.out.println("" + followers.length + " notifications envoyées vers NOTIFICATION_QUEUE");

        } catch (JMSException e) {
            System.err.println("Erreur : " + e.getMessage());
        }
    }
}