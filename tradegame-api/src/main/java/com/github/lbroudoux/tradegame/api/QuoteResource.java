package com.github.lbroudoux.tradegame.api;

import com.github.lbroudoux.tradegame.model.CollectionNames;
import com.github.lbroudoux.tradegame.model.Quote;

import io.quarkus.redis.datasource.RedisDataSource;
import io.quarkus.scheduler.Scheduled;
import org.jboss.logging.Logger;

import javax.inject.Inject;
import javax.ws.rs.Consumes;
import javax.ws.rs.GET;
import javax.ws.rs.PUT;
import javax.ws.rs.Path;
import javax.ws.rs.PathParam;
import javax.ws.rs.Produces;
import javax.ws.rs.core.Context;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;
import javax.ws.rs.sse.OutboundSseEvent;
import javax.ws.rs.sse.Sse;
import javax.ws.rs.sse.SseBroadcaster;
import javax.ws.rs.sse.SseEventSink;
import java.time.LocalDateTime;

@Path("/api/quote")
@Consumes(MediaType.APPLICATION_JSON)
@Produces(MediaType.APPLICATION_JSON)
/**
 * REST resource for managing quotes.
 * @author laurent
 */
public class QuoteResource {

   /** Get a JBoss logging logger. */
   private final Logger logger = Logger.getLogger(getClass());

   @Inject
   RedisDataSource redisDataSource;

   private Sse sse = null;
   private SseBroadcaster sseBroadcaster = null;
   private OutboundSseEvent.Builder eventBuilder;

   @PUT
   @Path("/{symbol}")
   public Response updateQuotePrice(@PathParam("symbol") String symbol, Double price) {
      Quote currentQuote = redisDataSource.hash(Quote.class).hget(CollectionNames.QUOTES, symbol);
      if (currentQuote != null) {
         redisDataSource.hash(Quote.class).hset(CollectionNames.QUOTES, symbol, new Quote(symbol, price));
         return Response.ok(price).build();
      }
      return Response.status(Response.Status.NOT_FOUND).build();
   }

   @GET
   @Path("/{symbol}")
   public Response getQuotePrice(@PathParam("symbol") String symbol) {
      Quote currentQuote = redisDataSource.hash(Quote.class).hget(CollectionNames.QUOTES, symbol);
      if (currentQuote != null) {
         return Response.ok(currentQuote.getPrice()).build();
      }
      return Response.status(Response.Status.NOT_FOUND).build();
   }

   @GET
   @Path("/streaming")
   @Produces(MediaType.SERVER_SENT_EVENTS)
   public void consume(@Context SseEventSink sseEventSink, @Context Sse sse) {
      if (this.sse == null) {
         this.sse = sse;
         this.eventBuilder = sse.newEventBuilder();
      }
      if (this.sseBroadcaster == null) {
         this.sseBroadcaster = sse.newBroadcaster();
      }
      logger.info("Registering a new sseEventSink: " + sseEventSink);
      sseBroadcaster.register(sseEventSink);
   }

   @Scheduled(every = "5s")
   public void publishQuotePrices() {
      logger.info("Publishing quote prices at " + LocalDateTime.now());

      if (sseBroadcaster != null) {
         // Publish a JSON array with quote prices.
         StringBuilder data = new StringBuilder("[");
         redisDataSource.hash(Quote.class).hvals(CollectionNames.QUOTES).stream()
               .forEach(quote -> data.append(serializeQuoteToJSON(quote)).append(", "));

         // Remove trailing ","
         data.deleteCharAt(data.length() - 2);
         data.append("]");

         logger.debug("Publishing: " + data.toString());
         OutboundSseEvent sseEvent = this.eventBuilder.name("message")
               .id(LocalDateTime.now().toString())
               .mediaType(MediaType.APPLICATION_JSON_TYPE)
               .data(data.toString())
               .reconnectDelay(3000)
               .build();
         sseBroadcaster.broadcast(sseEvent);
      }
   }

   private String serializeQuoteToJSON(Quote quote) {
      return "{\"symbol\": \"" + quote.getSymbol() + "\", \"price\": " + quote.getPrice() + "}";
   }
}
