package com.joshmahony.bigc;

import com.mongodb.BasicDBObject;
import com.mongodb.DB;
import com.mongodb.DBCollection;
import com.mongodb.MongoClient;
import lombok.extern.log4j.Log4j2;
import orestes.bloomfilter.redis.BloomFilterRedis;
import org.jsoup.nodes.Document;

import java.net.URL;

@Log4j2
public class HTMLStore {

    /**
     * Stores a connection to MongoDB
     */
    private final static MongoClient MONGO_CONNECTION = Mongo.getConnection();

    /**
     * Holds an instance to the singleton
     */
    private static HTMLStore instance;

    private static BloomFilterRedis<String> bloomFilter;

    /**
     *
     *  Singleton
     *
     */
    protected HTMLStore() {

        String IP = "localhost";

        bloomFilter = new BloomFilterRedis<>(IP, 6379, 1437758757, 0.01);

    }

    /**
     *
     * Returns an instance of HTMLStore
     *
     * @return The single instance of the store
     */
    public static HTMLStore getInstance() {

        if (instance == null) {

            instance = new HTMLStore();

        }

        return instance;

    }

    /**
     * Check see to if a URL already has a database entry
     * @param url
     * @return
     */
    private boolean hasURLInDatabase(URL url) {

        return getCollection(C.HTML_STORE_COLLECTION).find(new BasicDBObject("url", url.toString())).limit(1).size() > 0;

    }

    /**
     * TODO: Make this only store a document if its changed
     * @param url
     * @param d
     */
    public void store(URL url, Document d) throws URLDiscoveredException {

        if (checkDiscovered(url))
            throw new URLDiscoveredException("URL has already been seen");

        log.info("Storing HTML " + url.toString());

        // Get the HTML Store collection
        DBCollection collection = getCollection(C.HTML_STORE_COLLECTION);

        Long now = System.currentTimeMillis();

        BasicDBObject query = new BasicDBObject();

        query.append("url", url.toString());

        query.append("time", now.toString());

        query.append("html", d.toString());

        collection.insert(query);

        insertDiscovered(url);

    }

    private synchronized void insertDiscovered(URL url) {

        long start = System.currentTimeMillis();

        bloomFilter.add(url.toString());

        long end = System.currentTimeMillis();

        log.info("Bloom filter insert took: " + (end - start) + "ms");

    }

    public synchronized static boolean checkDiscovered(URL url) {

        long start = System.currentTimeMillis();

        boolean contains = bloomFilter.contains(url.toString());

        long end = System.currentTimeMillis();

        log.info("Bloom filter check took: " + (end - start) + "ms");

        return contains;

    }

    /**
     * Returns a given collection
     * @param collectionName
     * @return
     */
    public DBCollection getCollection(String collectionName) {

        // Get a connection to the database
        DB db = MONGO_CONNECTION.getDB(C.MONGO_DATABASE);

        // Get the crawl queue collection
        DBCollection collection = db.getCollection(collectionName);

        return collection;

    }


}
