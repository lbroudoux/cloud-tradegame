package com.github.lbroudoux.tradegame.api;

import com.github.lbroudoux.tradegame.model.CollectionNames;
import com.github.lbroudoux.tradegame.QuoteGameApp;
import com.github.lbroudoux.tradegame.model.Portfolio;
import com.github.lbroudoux.tradegame.model.User;

import io.quarkus.redis.datasource.RedisDataSource;
import io.quarkus.redis.datasource.hash.HashCommands;
import org.jboss.logging.Logger;

import javax.inject.Inject;
import javax.ws.rs.Consumes;
import javax.ws.rs.DELETE;
import javax.ws.rs.GET;
import javax.ws.rs.POST;
import javax.ws.rs.Path;
import javax.ws.rs.PathParam;
import javax.ws.rs.Produces;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;
import java.util.List;
import java.util.stream.Collectors;

@Path("/api/user")
@Consumes(MediaType.APPLICATION_JSON)
@Produces(MediaType.APPLICATION_JSON)
/**
 * REST resource for managing users.
 * @author laurent
 */
public class UserResource {

   /** Get a JBoss logging logger. */
   private final Logger logger = Logger.getLogger(getClass());

   @Inject
   RedisDataSource redisDataSource;

   @Inject
   QuoteGameApp application;

   @POST
   public Response register(User user) {
      application.ensureStart();
      logger.debug("Try creating user using cache tradegame-users");

      HashCommands<String, String, User> userCmd = redisDataSource.hash(User.class);
      User existingUser = userCmd.hget(CollectionNames.USERS, user.getName());

      if (existingUser == null) {
         userCmd.hset(CollectionNames.USERS, user.getName(), user);

         HashCommands<String, String, Portfolio> portfolioCmd = redisDataSource.hash(Portfolio.class);
         portfolioCmd.hset(CollectionNames.PORTFOLIOS, user.getName(), new Portfolio(user.getName(), 1000D));
         return Response.ok(user).build();
      }
      return Response.status(400, "User with same name already registered").build();
   }

   @GET
   public List<User> getAllUsers() {
      application.ensureStart();
      HashCommands<String, String, User> userCmd = redisDataSource.hash(User.class);
      return userCmd.hvals(CollectionNames.USERS).stream()
            .collect(Collectors.toList());
   }

   @DELETE
   @Path("/{name}")
   public Response unregisterUser(@PathParam("name") String name) {
      logger.debugf("Removing {} from caches", name);
      redisDataSource.hash(User.class).hdel(CollectionNames.USERS, name);
      redisDataSource.hash(Portfolio.class).hdel(CollectionNames.PORTFOLIOS, name);
      return Response.ok().build();
   }
}
