package com.tpjms;

import javax.jms.*;
import org.apache.activemq.ActiveMQConnectionFactory;

/**Ce service publie les événements liés aux posts quand par ex un utilisateur
 * crée un post, like, commente*/

public class PostEventPublisher{

    public static void main(String[] args) throws Exception {

        ConnectionFactory factory =
                new ActiveMQConnectionFactory("tcp://localhost:61616");

        Connection connection = factory.createConnection();
        Session session =
                connection.createSession(false, Session.AUTO_ACKNOWLEDGE);

        // Topic pour tous les événements de posts
        Topic topic = session.createTopic("POST_EVENTS");

        MessageProducer producer = session.createProducer(topic);

        connection.start();

        // Simulation : Est a crée un post
        String userId = "est_rvp";
        String postId = "post_100";
        String action = "POST_CREATED";

        TextMessage message = session.createTextMessage(
                action + "|userId:" + userId + "|postId:" + postId + "|content:Ma nouvelle photo de voyage!"
        );

        producer.send(message);

        System.out.println( "Événement publié sur POST_EVENTS : " + message.getText());;
    }
}