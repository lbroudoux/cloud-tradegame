package com.github.lbroudoux.tradegame.priceupdater;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.api.core.ApiFuture;
import com.google.api.core.ApiFutureCallback;
import com.google.api.core.ApiFutures;
import com.google.api.gax.core.CredentialsProvider;
import com.google.cloud.pubsub.v1.Publisher;
import com.google.common.util.concurrent.MoreExecutors;
import com.google.protobuf.ByteString;
import com.google.pubsub.v1.PubsubMessage;
import com.google.pubsub.v1.TopicName;
import io.quarkus.runtime.StartupEvent;
import org.eclipse.microprofile.config.inject.ConfigProperty;
import org.jboss.logging.Logger;

import javax.enterprise.context.ApplicationScoped;
import javax.enterprise.event.Observes;
import javax.inject.Inject;

@ApplicationScoped
/**
 * A utility bean for connecting and publishing on Google PubSub topic.
 * @author laurent
 */
public class GooglePubSubOrderListSnapshotPublisherManager {

   /** Get a JBoss logging logger. */
   private final Logger logger = Logger.getLogger(getClass());

   @Inject
   ObjectMapper mapper;

   @Inject
   CredentialsProvider credentialsProvider;

   @ConfigProperty(name = "quarkus.google.cloud.project-id")
   String projectId;

   @ConfigProperty(name = "tradegame-workingmemory.topic-name")
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

   public void publish(OrderListSnapshot snapshot) {
      try {
         // Serialize and convert to bytes the snapshot.
         String jsonMessage = mapper.writeValueAsString(snapshot);
         ByteString data = ByteString.copyFromUtf8(jsonMessage);

         // Build a PubSub message that should be ordered.
         PubsubMessage pubsubMessage = PubsubMessage.newBuilder()
               .setData(data)
               .setOrderingKey("snapshots")
               .build();

         ApiFuture<String> messageIdFuture = publisher.publish(pubsubMessage);
         ApiFutures.addCallback(messageIdFuture, new ApiFutureCallback<String>() {
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
