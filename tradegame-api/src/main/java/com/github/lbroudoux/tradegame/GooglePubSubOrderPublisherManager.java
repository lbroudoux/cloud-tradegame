package com.github.lbroudoux.tradegame;

import com.github.lbroudoux.tradegame.model.Order;

import com.google.api.core.ApiFuture;
import com.google.api.core.ApiFutureCallback;
import com.google.api.core.ApiFutures;
import com.google.api.gax.core.CredentialsProvider;
import com.google.cloud.pubsub.v1.Publisher;
import com.google.common.util.concurrent.MoreExecutors;
import com.google.protobuf.ByteString;
import com.google.pubsub.v1.PubsubMessage;
import com.google.pubsub.v1.TopicName;

import com.fasterxml.jackson.databind.ObjectMapper;

import io.quarkus.runtime.StartupEvent;
import org.eclipse.microprofile.config.inject.ConfigProperty;
import org.jboss.logging.Logger;

import javax.enterprise.context.ApplicationScoped;
import javax.enterprise.event.Observes;
import javax.inject.Inject;

@ApplicationScoped
/**
 * A utility bean for connecting and publishing on Google PuSub topic.
 * @author laurent
 */
public class GooglePubSubOrderPublisherManager {

   /** Get a JBoss logging logger. */
   private final Logger logger = Logger.getLogger(getClass());

   @Inject
   ObjectMapper mapper;

   @Inject
   CredentialsProvider credentialsProvider;

   @ConfigProperty(name = "quarkus.google.cloud.project-id")
   String projectId;

   @ConfigProperty(name = "tradegame-orders.topic-name")
   String topic;

   Publisher publisher;

   void onStart(@Observes StartupEvent ev) {
      try {
         // Build a new publisher using topic, project id and credentials.
         TopicName topicName = TopicName.of(projectId, topic);
         publisher = Publisher.newBuilder(topicName)
               .setCredentialsProvider(credentialsProvider)
               .setEnableMessageOrdering(true)
               .build();
      } catch (Exception e) {
         throw new RuntimeException("Unable to connect to Google PubSub topic " + topic + " on project " + projectId, e);
      }
   }

   public void publish(Order order) {
      try {
         // Serialize and convert to bytes the order.
         String jsonMessage = mapper.writeValueAsString(order);
         ByteString data = ByteString.copyFromUtf8(jsonMessage);

         // Build a PubSub message that should be ordered.
         PubsubMessage pubsubMessage = PubsubMessage.newBuilder()
               .setData(data)
               .setOrderingKey("orders")
               .build();

         ApiFuture<String> messageIdFuture = publisher.publish(pubsubMessage);// Publish the message
         ApiFutures.addCallback(messageIdFuture, new ApiFutureCallback<String>() {// Wait for message submission and log the result
            public void onSuccess(String messageId) {
               logger.debugv("published with message id {0}", messageId);
            }
            public void onFailure(Throwable t) {
               logger.debugv("failed to publish: {0}", t);
            }
         }, MoreExecutors.directExecutor());
      } catch (Exception e) {
         logger.error("Exception while publishing message on PubSub", e);
      }
   }
}
