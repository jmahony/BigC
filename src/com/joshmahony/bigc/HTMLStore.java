package com.joshmahony.bigc;

import com.mongodb.BasicDBObject;
import com.mongodb.DBCollection;
import lombok.extern.log4j.Log4j2;
import org.jsoup.nodes.Document;

import java.net.URL;

/**
 * Responbile for saving HTML to the database
 */
@Log4j2
public class HTMLStore {

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
     *
     * Saves a document to MongoDB
     *
     * @param url the URL of the page
     * @param d the document to save
     */
    public void store(URL url, Document d) throws URLDiscoveredException {

        if (Discovered.check(url))
            throw new URLDiscoveredException("URL has already been seen");

        log.info("Storing HTML " + url.toString());

        DBCollection collection = Mongo.getCollection(C.HTML_STORE_COLLECTION);

        Long now = System.currentTimeMillis();

        BasicDBObject query = new BasicDBObject();

        query.append("url", url.toString());

        query.append("time", now.toString());

        query.append("html", d.toString());

        collection.insert(query);

        Discovered.insert(url);

    }

}
