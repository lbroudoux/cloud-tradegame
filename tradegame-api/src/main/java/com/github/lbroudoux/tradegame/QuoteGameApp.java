package com.github.lbroudoux.tradegame;

import com.github.lbroudoux.tradegame.model.CollectionNames;
import com.github.lbroudoux.tradegame.model.Quote;
import io.quarkus.redis.datasource.RedisDataSource;
import io.quarkus.redis.datasource.hash.HashCommands;
import io.quarkus.runtime.StartupEvent;
import org.jboss.logging.Logger;

import javax.enterprise.context.ApplicationScoped;
import javax.enterprise.event.Observes;
import javax.inject.Inject;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

@ApplicationScoped
/**
 * @author laurent
 */
public class QuoteGameApp {

   /** Get a JBoss logging logger. */
   private final Logger logger = Logger.getLogger(getClass());

   private CountDownLatch waitUntilStarted = new CountDownLatch(1);

   @Inject
   RedisDataSource redisDataSource;

   void onStart(@Observes StartupEvent ev) {
      logger.info("Create or get caches named tradegame-users, tradegame-portfolios with the default configuration");

      HashCommands<String, String, Quote> quoteCmd = redisDataSource.hash(Quote.class);
      // Put initialization values if none.
      if (!quoteCmd.hexists(CollectionNames.QUOTES, "TYR")) {
         quoteCmd.hset(CollectionNames.QUOTES, "TYR", new Quote("TYR", 187.71));
         quoteCmd.hset(CollectionNames.QUOTES, "CYB", new Quote("CYB", 140.57));
      }
      waitUntilStarted.countDown();
   }

   public void ensureStart() {
      try {
         if (!waitUntilStarted.await(5, TimeUnit.SECONDS)) {
            throw new RuntimeException(new TimeoutException());
         }
      } catch (InterruptedException e) {
         throw new RuntimeException(e);
      }
   }
}
