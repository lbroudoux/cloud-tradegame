package com.github.lbroudoux.tradegame.processors;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.github.lbroudoux.tradegame.model.CollectionNames;
import com.github.lbroudoux.tradegame.model.Order;
import com.github.lbroudoux.tradegame.model.OrderType;
import com.github.lbroudoux.tradegame.model.Portfolio;

import com.google.api.gax.core.CredentialsProvider;
import com.google.cloud.pubsub.v1.MessageReceiver;
import com.google.cloud.pubsub.v1.Subscriber;
import com.google.cloud.pubsub.v1.SubscriptionAdminClient;
import com.google.cloud.pubsub.v1.SubscriptionAdminSettings;
import com.google.pubsub.v1.*;

import io.micrometer.core.annotation.Counted;
import io.quarkus.redis.datasource.RedisDataSource;
import io.quarkus.redis.datasource.hash.HashCommands;
import io.quarkus.runtime.StartupEvent;
import org.eclipse.microprofile.config.inject.ConfigProperty;
import org.jboss.logging.Logger;

import javax.enterprise.context.ApplicationScoped;
import javax.enterprise.event.Observes;
import javax.inject.Inject;
import java.io.IOException;
import java.util.Optional;
import java.util.stream.StreamSupport;

@ApplicationScoped
/**
 * @author laurent
 */
public class PortfolioUpdater {

   /** Get a JBoss logging logger. */
   private final Logger logger = Logger.getLogger(getClass());

   private final String SUBSCRIPTION_NAME = "orders-portfolio-updater";

   @Inject
   ObjectMapper mapper;

   @Inject
   CredentialsProvider credentialsProvider;

   @Inject
   RedisDataSource redisDataSource;

   @ConfigProperty(name = "quarkus.google.cloud.project-id")
   String projectId;

   @ConfigProperty(name = "tradegame-orders.topic-name")
   String topic;

   void onStart(@Observes StartupEvent ev) {
      try {
         // Build a new receiver using topic, project id and credentials.
         TopicName topicName = TopicName.of(projectId, topic);
         // Subscribe to PubSub
         MessageReceiver receiver = (message, consumer) -> {
            logger.infov("Got message {0}", message.getData().toStringUtf8());
            try {
               updatePortfolio(mapper.readValue(message.getData().toStringUtf8(), Order.class));
            } catch (Exception e) {
               logger.error("Exception while updating the portfolio with Order", e);
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
      // List all existing subscriptions and create the subscription if needed.
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

   @Counted(value = "processedOrders", description = "How many orders have been processed.")
   public void updatePortfolio(Order order) {
      logger.info("Get new order to process...");
      HashCommands<String, String, Portfolio> portfolioCmd = redisDataSource.hash(Portfolio.class);

      Portfolio portfolio = portfolioCmd.hget(CollectionNames.PORTFOLIOS, order.getUsername());

      if (portfolio != null) {
         logger.infof("Processing order %d for user %s", order.getTimestamp(), order.getUsername());

         Double totalPrice = order.getPrice() * order.getNumber();
         Long numberOfQuotes = portfolio.getQuotes().get(order.getQuote());
         if (numberOfQuotes == null) {
            numberOfQuotes = 0L;
         }

         if (order.getOrderType().equals(OrderType.BUY)) {
            portfolio.setMoney(portfolio.getMoney() - totalPrice);
            portfolio.getQuotes().put(order.getQuote(), numberOfQuotes + order.getNumber());
         } else {
            portfolio.setMoney(portfolio.getMoney() + totalPrice);
            portfolio.getQuotes().put(order.getQuote(), numberOfQuotes - order.getNumber());
         }
         portfolioCmd.hset(CollectionNames.PORTFOLIOS, order.getUsername(), portfolio);
      }
   }
}
