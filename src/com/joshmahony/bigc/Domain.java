package com.joshmahony.bigc;

import com.mongodb.*;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.json.simple.JSONObject;

import java.net.MalformedURLException;
import java.net.URL;
import java.util.HashSet;

/**
 * Created by joshmahony on 16/12/2013.
 */
public class Domain {

    /**
     *
     */
    private long lastCrawlTime = 0;

    /**
     *
     */
    private long crawlRate;

    /**
     *
     */
    private URL url;

    /**
     *
     */
    public boolean isEmpty;

    /**
     * Store the connection connection to MongoDB
     */
    public MongoClient connection;

    /**
     * Creates an instance of log4j
     */
    private final Logger logger;

    /**
     *
     * @param _domain
     * @param _connection
     */
    public Domain(String _domain, MongoClient _connection, Object _settings) throws MalformedURLException {

        logger = LogManager.getLogger(Domain.class.getName());

        url = new URL(_domain);

        connection = _connection;

        SettingsParser sp = new SettingsParser(url, (JSONObject) _settings);

        crawlRate = sp.getCrawlRate();

        createDomainQueue();

    }

    public Domain(String _domain, MongoClient _pool) throws MalformedURLException {

        logger = LogManager.getLogger(Domain.class.getName());

        url = new URL(_domain);

        connection = _pool;

        crawlRate = C.DEFAULT_CRAWL_RATE;

        createDomainQueue();

    }

    /**
     *
     */
    private void createDomainQueue() {

        // Make sure the domain doesn't already have a document
        if (hasQueueInDatabase()){

            logger.info(getDomain() + " Already has domain queue in database");

            return;

        }

        logger.info("Creating new domain queue for " + getDomain() + " in database");

        // Get the crawl queue collection
        DBCollection collection = getCollection(C.CRAWL_QUEUE_COLLECTION);

        // Create the domainQueue
        BasicDBObject domainQueue = new BasicDBObject();

        domainQueue.append("domain", getDomain());

        //TODO: Add query string?
        domainQueue.append("queue", new String[] {"/"});

        //Insert queue into database
        collection.insert(domainQueue);

    }

    /**
     * Enqueues a list of domains for the given domain
     * @param urls
     */
    public synchronized void enqueueURLs(HashSet<String> urls) {

        // Get the crawl queue collection
        DBCollection collection = getCollection(C.CRAWL_QUEUE_COLLECTION);

        // Build the query
        BasicDBObject domainQueueQuery = new BasicDBObject("domain", getDomain());

        // Build the set to add to the queue
        BasicDBObject domainQueueUpdate = new BasicDBObject(
            "$addToSet", new BasicDBObject(
                "queue", new BasicDBObject(
                    "$each", urls.toArray()
                )
            )
        );

        // Update the domains queue
        collection.update(domainQueueQuery, domainQueueUpdate);

    }


    /**
     * Returns a the next URL for an available domain
     * @return URL || null
     */
    public synchronized URL getNextURL() throws CrawlQueueEmptyException {

        try {

            // Get the crawl queue collection
            DBCollection collection = getCollection(C.CRAWL_QUEUE_COLLECTION);

            // Fetch the document, this will retrieve the whole document and then pop one url from the queue
            DBObject result = collection.findAndModify(new BasicDBObject("domain", getDomain()), new BasicDBObject("$pop", new BasicDBObject("queue", -1)));

            // Get the domains queue
            BasicDBList queue = (BasicDBList) result.get("queue");

            // Check the queue isn't empty
            if (queue.size() <= 0) {
                isEmpty = true;
                throw new CrawlQueueEmptyException("The crawl queue for " + getDomain() + " is empty!");
            }

            setLastCrawlTime(System.currentTimeMillis());

            // return the url
            return new URL("http://" + getDomain() + queue.get(0));

        } catch (MalformedURLException e) {

            logger.warn("A malformed URL was added to the queue, URL: " + e.getMessage());

        }

        return null;

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

    /**
     * Checks to see if the domain already has a document in the database
     * @return
     */
    private synchronized boolean hasQueueInDatabase() {

        return getCollection(C.CRAWL_QUEUE_COLLECTION).find(new BasicDBObject("domain", getDomain())).limit(1).size() > 0;

    }

    /**
     *
     * @return
     */
    public long getLastCrawlTime() {
        return lastCrawlTime;
    }

    /**
     *
     * @param m
     */
    private void setLastCrawlTime(long m) {
        lastCrawlTime = m;
    }

    /**
     * Get the crawl rate for a domain
     * @return
     */
    public long getCrawlRate() {
        return crawlRate;
    }

    /**
     *
     * @return
     */
    public String getDomain() {
        return url.getHost();
    }

}

