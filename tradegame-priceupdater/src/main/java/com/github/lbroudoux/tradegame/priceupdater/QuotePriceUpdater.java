package com.github.lbroudoux.tradegame.priceupdater;


import com.github.lbroudoux.tradegame.model.CollectionNames;
import com.github.lbroudoux.tradegame.model.Order;
import com.github.lbroudoux.tradegame.model.Quote;

import com.google.api.gax.core.CredentialsProvider;
import com.google.cloud.pubsub.v1.MessageReceiver;
import com.google.cloud.pubsub.v1.Subscriber;
import com.google.cloud.pubsub.v1.SubscriptionAdminClient;
import com.google.cloud.pubsub.v1.SubscriptionAdminSettings;
import com.google.pubsub.v1.*;

import com.fasterxml.jackson.databind.ObjectMapper;
import io.quarkus.redis.datasource.RedisDataSource;
import io.quarkus.redis.datasource.hash.HashCommands;
import io.quarkus.runtime.StartupEvent;
import org.eclipse.microprofile.config.inject.ConfigProperty;
import org.jboss.logging.Logger;
import org.kie.api.runtime.KieRuntimeBuilder;
import org.kie.api.runtime.KieSession;
import org.kie.api.runtime.rule.FactHandle;

import javax.enterprise.context.ApplicationScoped;
import javax.enterprise.event.Observes;
import javax.inject.Inject;
import javax.inject.Named;
import javax.ws.rs.Consumes;
import javax.ws.rs.POST;
import javax.ws.rs.Path;
import javax.ws.rs.Produces;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;
import java.io.IOException;
import java.util.Optional;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.stream.StreamSupport;

@Path("/api/order")
@Consumes(MediaType.APPLICATION_JSON)
@Produces(MediaType.APPLICATION_JSON)
@ApplicationScoped
/**
 * Component that takes care of processing orders and updating portfolio
 * @author laurent
 */
public class QuotePriceUpdater {

   /** Get a JBoss logging logger. */
   private final Logger logger = Logger.getLogger(getClass());

   private final String SUBSCRIPTION_NAME = "workingmemory-updater";

   @Inject
   ObjectMapper mapper;

   @Inject
   RedisDataSource redisDataSource;

   @Inject
   CredentialsProvider credentialsProvider;

   @Inject
   GooglePubSubOrderListSnapshotPublisherManager publisherManager;

   @ConfigProperty(name = "quarkus.google.cloud.project-id")
   String projectId;

   @ConfigProperty(name = "quotegame-workingmemory.topic-name")
   String topic;

   private KieSession ksession;

   private ConcurrentLinkedQueue<FactHandle> lastOrdersHandles = new ConcurrentLinkedQueue<>();

   @Inject
   @Named("quotePriceKS")
   QuotePriceUpdater( KieRuntimeBuilder runtimeBuilder ) {
      ksession = runtimeBuilder.newKieSession();
   }

   void onStart(@Observes StartupEvent ev) {
      try {
         // Build a new receiver using topic, project id and credentials.
         TopicName topicName = TopicName.of(projectId, topic);
         // Subscribe to PubSub
         MessageReceiver receiver = (message, consumer) -> {
            logger.infov("Got message {0}", message.getData().toStringUtf8());
            try {
               syncWorkingMemory(mapper.readValue(message.getData().toStringUtf8(), OrderListSnapshot.class));
            } catch (Exception e) {
               logger.error("Exception while trying to parse message as JSON OrderListSnapshot", e);
            }
            consumer.ack();
         };

         // Initialize subscription and plug-in the receiver.
         ProjectSubscriptionName subscriptionName = initSubscription(topicName);
         logger.info("Registering subscriber to Google PubSub subs " + SUBSCRIPTION_NAME + " on topic " + topic);
         Subscriber subscriber = Subscriber.newBuilder(subscriptionName, receiver)
               .setCredentialsProvider(credentialsProvider)
               .build();
         subscriber.startAsync().awaitRunning();
      } catch (Exception e) {
         throw new RuntimeException("Unable to connect to Google PubSub topic " + topic + " on project " + projectId, e);
      }
   }

   private ProjectSubscriptionName initSubscription(TopicName topicName) throws IOException {
      logger.info("Initializing Google PubSub subs " + SUBSCRIPTION_NAME + " to topic " + topic);
      // List all existing subscriptions and create the 'test-subscription' if needed.
      ProjectSubscriptionName subscriptionName = ProjectSubscriptionName.of(projectId, SUBSCRIPTION_NAME);
      SubscriptionAdminSettings subscriptionAdminSettings = SubscriptionAdminSettings.newBuilder()
            .setCredentialsProvider(credentialsProvider)
            .build();
      try (SubscriptionAdminClient subscriptionAdminClient = SubscriptionAdminClient.create(subscriptionAdminSettings)) {
         Iterable<Subscription> subscriptions = subscriptionAdminClient.listSubscriptions(ProjectName.of(projectId))
               .iterateAll();
         Optional<Subscription> existing = StreamSupport.stream(subscriptions.spliterator(), false)
               .filter(sub -> sub.getName().equals(subscriptionName.toString()))
               .findFirst();
         if (!existing.isPresent()) {
            logger.info("Subscription " + SUBSCRIPTION_NAME + " doesn't exist. Creating it.");
            subscriptionAdminClient.createSubscription(subscriptionName, topicName, PushConfig.getDefaultInstance(), 10);
         }
      }
      return subscriptionName;
   }

   @POST
   public Response considerOrder(Order order) {
      logger.info("Get new order to process...");

      logger.info("Get corresponding Quote ...");
      HashCommands<String, String, Quote> quoteCmd = redisDataSource.hash(Quote.class);
      Quote quote = quoteCmd.hget(CollectionNames.QUOTES, order.getQuote());
      int quoteBefore = quote.hashCode();

      logger.debug("Inserting Order fact: " + order);
      FactHandle orderFH = ksession.insert(order);
      lastOrdersHandles.add(orderFH);

      logger.debug("Inserting Quote fact ...");
      FactHandle quoteFH = ksession.insert(quote);

      logger.debug("Fire Rules ...");
      int rulesFired = ksession.fireAllRules();
      logger.debug("Number of rules fired = " + rulesFired);

      int quoteAfter = quote.hashCode();

      if (quoteBefore == quoteAfter){
         logger.info("Quote not modified");
      } else {
         logger.info("Updating Quote cache");
         quoteCmd.hset(CollectionNames.QUOTES, order.getQuote(), quote);
      }

      ksession.delete(quoteFH);
      logger.info("Number of facts in WM = " + ksession.getFactCount());
      if (ksession.getFactCount() > 5) {
         ksession.delete(lastOrdersHandles.remove());
      }

      // Now that we updated the status from working memory (either from the rules or by cleaning
      // the number of fact counts), we should build a snapshot and publish it on Kafka.
      publishWorkingMemorySnapshot();

      logger.info("Returning response ok");
      return Response.ok(order).build();
   }

   protected void publishWorkingMemorySnapshot() {
      logger.info("Publishing a new WorkingMemory snapshot...");
      OrderListSnapshot snapshot = new OrderListSnapshot();
      snapshot.setTimestamp(System.currentTimeMillis());
      for (FactHandle handle : lastOrdersHandles) {
         Object object = ksession.getObject(handle);
         if (object instanceof Order) {
            snapshot.addOrder((Order)object);
         }
      }
      publisherManager.publish(snapshot);
   }

   protected void syncWorkingMemory(OrderListSnapshot snapshot) {
      logger.info("Receiving a new WM snapshot produced at " + snapshot.getTimestamp());
      synchronized (ksession) {
         // Remove alder facts and add new ones.
         for (FactHandle handle : lastOrdersHandles) {
            ksession.delete(handle);
         }
         lastOrdersHandles.clear();
         for (Order order : snapshot.getOrders()) {
            logger.info("Syncing a new Order fact: " + order.toString());
            FactHandle orderFH = ksession.insert(order);
            lastOrdersHandles.add(orderFH);
         }
         logger.info("Number of facts synced in WM = " + ksession.getFactCount());
      }
   }
}
