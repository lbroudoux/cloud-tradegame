package com.github.lbroudoux.tradegame.api;

import com.github.lbroudoux.tradegame.model.CollectionNames;
import com.github.lbroudoux.tradegame.model.Portfolio;

import io.quarkus.redis.datasource.RedisDataSource;
import org.jboss.logging.Logger;

import javax.inject.Inject;
import javax.ws.rs.Consumes;
import javax.ws.rs.GET;
import javax.ws.rs.Path;
import javax.ws.rs.PathParam;
import javax.ws.rs.Produces;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;
import java.util.List;
import java.util.stream.Collectors;

@Path("/api/portfolio")
@Consumes(MediaType.APPLICATION_JSON)
@Produces(MediaType.APPLICATION_JSON)
/**
 * REST resource for managing portfolios.
 * @author laurent
 */
public class PortfolioResource {

   /** Get a JBoss logging logger. */
   private final Logger logger = Logger.getLogger(getClass());

   @Inject
   RedisDataSource redisDataSource;

   @GET
   @Path("/{username}")
   public Response getByUser(@PathParam("username") String username) {
      logger.infof("Retrieving portfolio for user '%s'", username);
      Portfolio portfolio =  redisDataSource.hash(Portfolio.class).hget(CollectionNames.PORTFOLIOS, username);
      if (portfolio != null) {
         return Response.ok(portfolio).build();
      }
      return Response.status(Response.Status.NOT_FOUND).build();
   }

   @GET
   public List<Portfolio> getAllPortfolios() {
      return redisDataSource.hash(Portfolio.class).hvals(CollectionNames.PORTFOLIOS).stream()
            .collect(Collectors.toList());
   }

   @GET
   @Path("/tops")
   public List<Portfolio> getTopPortfolios() {
      return redisDataSource.hash(Portfolio.class).hvals(CollectionNames.PORTFOLIOS).stream()
            .sorted((p1, p2) -> {
               return (p2.getMoney() > p1.getMoney()) ? -1 : 1;
            })
            .limit(5)
            .collect(Collectors.toList());
   }
}
