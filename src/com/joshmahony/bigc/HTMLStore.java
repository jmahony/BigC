package com.joshmahony.bigc;

import com.mongodb.BasicDBObject;
import com.mongodb.DB;
import com.mongodb.DBCollection;
import com.mongodb.MongoClient;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.jsoup.nodes.Document;

import java.net.URL;
import java.net.UnknownHostException;

public class HTMLStore {

    /**
     * Stores a connection to MongoDB
     */
    private static MongoClient connection;

    /**
     * Creates an instance of log4j
     */
    private final Logger logger;

    /**
     *  Initialise the HTML Store
     */
    public HTMLStore() {

        logger = LogManager.getLogger(HTMLStore.class.getName());

        initMongoConnection();

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
    public void store(URL url, Document d) {

        // Get the HTML Store collection
        DBCollection collection = getCollection(C.HTML_STORE_COLLECTION);

        // Create an object to store the HTML, key as the current timestamp, the HTML as the value
        BasicDBObject html = new BasicDBObject((new Long(System.currentTimeMillis())).toString(), d.toString());

        // If the URL already has an entry, append it to the array of entries
        if (hasURLInDatabase(url)) {

            BasicDBObject query = new BasicDBObject("url", url.toString());

            BasicDBObject update = new BasicDBObject("$addToSet", html);

            collection.update(query, update);

        // Else add a new entry
        } else {

            BasicDBObject query = new BasicDBObject();

            query.append("url", url.toString());

            query.append("html", html);

            collection.insert(query);

        }

    }

    /**
     * Creare a connection to MongoDB
     */
    private void initMongoConnection() {

        logger.info("Initialising MongoDB connection connection... ");

        try {

            connection = new MongoClient(C.MONGO_HOST, C.MONGO_PORT);

        } catch (UnknownHostException e) {

            logger.fatal("Could not connect to MongoDB server");

            System.exit(-1);

        }

    }

    /**
     * Returns a given collection
     * @param collectionName
     * @return
     */
    public DBCollection getCollection(String collectionName) {

        // Get a connection to the database
        DB db = connection.getDB(C.MONGO_DATABASE);

        // Get the crawl queue collection
        DBCollection collection = db.getCollection(collectionName);

        return collection;

    }


}