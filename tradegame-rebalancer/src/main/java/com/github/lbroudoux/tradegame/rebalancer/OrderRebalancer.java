package com.github.lbroudoux.tradegame.rebalancer;

import org.apache.camel.Exchange;
import org.apache.camel.builder.RouteBuilder;
import org.eclipse.microprofile.config.inject.ConfigProperty;
import org.jboss.logging.Logger;

import javax.enterprise.context.ApplicationScoped;
import java.util.List;

@ApplicationScoped
/**
 * Component that takes care of rebalancing orders to a unique REST consumer
 * @author laurent
 */
public class OrderRebalancer extends RouteBuilder {

   private final Logger logger = Logger.getLogger(getClass());

   @ConfigProperty(name = "quarkus.google.cloud.project-id")
   String projectId;

   @ConfigProperty(name = "price-updater.subscription-name")
   String subscription;

   @ConfigProperty(name = "price-updater.service-account-key")
   String serviceAccountKey;

   @ConfigProperty(name = "price-updater.endpoints", defaultValue = "localhost:8083")
   List<String> priceUpdaterEndpoints;

   @Override
   public void configure() throws Exception {

      String[] priceUpdateEndpointsArray = priceUpdaterEndpoints.toArray(new String[priceUpdaterEndpoints.size()]);

      from("google-pubsub:" + projectId + ":" + subscription + "?ackMode=AUTO"
            + "&serviceAccountKey=" + serviceAccountKey
            + "&exchangePattern=InOnly")
            .log("Got ${body}")
            .setHeader(Exchange.CONTENT_TYPE, constant("application/json"))
            .loadBalance()
            .failover()
            .to(priceUpdaterEndpoints.toArray(new String[0]));
   }
}
