package com.joshmahony.bigc;

import com.mongodb.BasicDBObject;
import com.mongodb.DB;
import com.mongodb.DBCollection;
import com.mongodb.MongoClient;
import lombok.extern.log4j.Log4j2;
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

    /**
     *
     *  Singleton
     *
     */
    protected HTMLStore() {}

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
    public void store(URL url, Document d) {

        // Get the HTML Store collection
        DBCollection collection = getCollection(C.HTML_STORE_COLLECTION);

        // Create an object to store the HTML, key as the current timestamp, the HTML as the value
        BasicDBObject html = new BasicDBObject((new Long(System.currentTimeMillis())).toString(), d.toString());

        // If the URL already has an entry, append it to the array of entries
        if (hasURLInDatabase(url)) {

            BasicDBObject query = new BasicDBObject("url", url.toString());

            BasicDBObject update = new BasicDBObject("$push", new BasicDBObject("html", html));

            collection.update(query, update);

        // Else add a new entry
        } else {

            BasicDBObject query = new BasicDBObject();

            query.append("url", url.toString());

            query.append("html", new BasicDBObject[] {html});

            collection.insert(query);

        }

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
