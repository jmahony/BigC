package com.joshmahony.bigc;

import com.mongodb.MongoClient;
import lombok.extern.log4j.Log4j2;

import java.net.UnknownHostException;

/**
 * Created by joshmahony on 20/04/2014.
 */
@Log4j2
public class Mongo {

    /**
     * MongoDB connection pool
     */
    private static MongoClient mongoConnection = null;

    protected Mongo() {}

    public static MongoClient getConnection() {

        if (mongoConnection == null) {

            try {

                mongoConnection =  new MongoClient(C.MONGO_HOST, C.MONGO_PORT);

            } catch (UnknownHostException e) {

                log.fatal("Could not connect to MongoDB");

            }


        }

        return mongoConnection;

    }

}
