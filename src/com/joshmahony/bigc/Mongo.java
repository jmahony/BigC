package com.joshmahony.bigc;

import com.mongodb.DB;
import com.mongodb.DBCollection;
import com.mongodb.MongoClient;
import lombok.extern.log4j.Log4j2;

import java.net.UnknownHostException;

/**
 * Used to hold the mongo connection
 */
@Log4j2
public class Mongo {

    /**
     * MongoDB connection pool
     */
    private static MongoClient mongoConnection = null;

    /**
     *
     * Singleton
     *
     */
    protected Mongo() {}

    /**
     *
     * Returns the MongoDB connection instance
     *
     * @return the connection
     */
    public static MongoClient getConnection() {

        if (mongoConnection == null) {

            try {

                mongoConnection =  new MongoClient(C.MONGO_HOST, C.MONGO_PORT);

            } catch (UnknownHostException e) {

                log.fatal("Could not connect to MongoDB");

                System.exit(0);

            }


        }

        return mongoConnection;

    }

    /**
     *
     * Returns a given collection
     *
     * @param collectionName the collection to get
     * @return the collection
     */
    public static DBCollection getCollection(String collectionName) {

        MongoClient connection = getConnection();

        // Get a connection to the database
        DB db = connection.getDB(C.MONGO_DATABASE);

        // Get the crawl queue collection
        DBCollection collection = db.getCollection(collectionName);

        return collection;

    }

}
