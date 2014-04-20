package com.joshmahony.bigc;

import com.mongodb.*;
import crawlercommons.fetcher.http.BaseHttpFetcher;
import crawlercommons.fetcher.http.UserAgent;
import crawlercommons.robots.BaseRobotRules;
import crawlercommons.robots.RobotUtils;
import crawlercommons.robots.SimpleRobotRulesParser;
import lombok.extern.log4j.Log4j2;
import org.json.simple.JSONObject;

import java.net.MalformedURLException;
import java.net.URL;
import java.util.HashSet;

/**
 * Created by joshmahony on 16/12/2013.
 */
@Log4j2
public class Domain {

    /**
     * The unix timestamp of the last time the crawler crawled this domain.
     */
    private long lastCrawlTime;

    /**
     * The rate at which this domain is allowed to be crawled.
     */
    private long crawlRate;

    /**
     * The URL of the domain (A bit confusing)
     */
    private URL url;

    /**
     * Stores whether the queue is empty or not
     * TODO: Do something when the queue is empty, possibly recrawl?
     */
    public boolean isEmpty;


    /**
     * Store the robots.txt rules
     */
    private BaseRobotRules rules;

    /**
     * Whether or not the robots.txt file has been fetched
     */
    private boolean hasRobots;

    /**
     * Takes in a domain name, a database connection and a settings object
     * @param _domain
     */
    public Domain(String _domain, Object _settings) throws MalformedURLException {

        lastCrawlTime = 0;

        hasRobots = false;

        url = new URL(_domain);

        SettingsParser sp = new SettingsParser(url, (JSONObject) _settings);

        crawlRate = sp.getCrawlRate();

        createDomainQueue();

    }

    /**
     * Takes in a domain name and a database connection, the crawl rate is set to the default rate
     * @param _domain
     * @throws MalformedURLException
     */
    public Domain(String _domain) throws MalformedURLException {

        lastCrawlTime = 0;

        hasRobots = false;

        url = new URL(_domain);

        crawlRate = C.DEFAULT_CRAWL_RATE;

        createDomainQueue();

    }

    /**
     * Creates a domain queue in the database
     */
    private void createDomainQueue() {

        // Make sure the domain doesn't already have a document
        if (hasQueueInDatabase()){

            log.debug(getDomain() + " Already has domain queue in database");

            return;

        }

        log.debug("Creating new domain queue for " + getDomain() + " in database");

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
     * Enqueues a list of domains for the domain
     * @param urls
     */
    public void enqueueURLs(HashSet<String> urls) {

        cleanURLs(urls);

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
     * Removes entries from a set of urls according to robots.txt
     * @param urls
     * @return
     */
    private HashSet<String> cleanURLs(HashSet<String> urls) {

        if (!hasRobots) getRobots();

        // Don't enqueue URL if its disallowed in robots.txt
        if (rules != null) {
            HashSet<String> remove = new HashSet<String>();

            for(String url : urls) {

                URL u = makeURLFromPath(url);

                boolean robotsAllowed = rules.isAllowed(u.toString());

                boolean alreadyCrawled = HTMLStore.checkDiscovered(u);

                if (alreadyCrawled) {

                    log.info(u.toString() + " not being added");

                }

                if (!robotsAllowed || alreadyCrawled || url.length() == 0) {
                    remove.add(url);
                }
            }

            urls.removeAll(remove);
        }

        return urls;

    }

    /**
     * Returns a the next URL for an available domain
     * @return URL || null
     */
    public synchronized URL getNextURL() throws CrawlQueueEmptyException {

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

        return makeURLFromPath(queue.get(0).toString());

    }

    /**
     * Makes a url from the path
     * @param path
     * @return
     */
    private URL makeURLFromPath(String path) {

        URL url = null;

        try {

            return new URL("http://" + getDomain() + path);

        } catch (MalformedURLException e) {

            log.warn("A malformed URL was added to the queue, URL: " + e.getMessage());

        }

        return url;
    }

    /**
     * Returns a given collection
     * @param collectionName
     * @return
     */
    public DBCollection getCollection(String collectionName) {

        MongoClient connection = Mongo.getConnection();

        // Get a connection to the database
        DB db = connection.getDB(C.MONGO_DATABASE);

        // Get the crawl queue collection
        DBCollection collection = db.getCollection(collectionName);

        return collection;

    }

    public boolean hasDocument(URL url) {

        DBCollection collection = getCollection(C.HTML_STORE_COLLECTION);

        DBCursor o = collection.find(new BasicDBObject("url", url.toString()));

        return o.size() > 0;

    }

    /**
     * Is this going to hang with every new domain created?!?!?!
     * TODO: run this in its own thread or create a job to fetch robots.txt OR!! make the crawler fetch it on its first crawl.
     */
    private void getRobots() {

        if (hasRobots) return;

        // Only get robots.txt if the domain is in the whitelist
        if (!CrawlQueue.getDomainWhiteList().contains(getDomain())) return;

        log.info("Fetching robots.txt for " + getDomain());

        UserAgent ua = new UserAgent("BigC", "jm426@uni.brighton.ac.uk", "joshmahony.com");

        BaseHttpFetcher bf = RobotUtils.createFetcher(ua, 1);

        SimpleRobotRulesParser parser = new SimpleRobotRulesParser();

        try {

           rules = RobotUtils.getRobotRules(bf, parser, new URL(url.toString() + "/robots.txt"));

           hasRobots = true;

        } catch (MalformedURLException e) {

            e.printStackTrace();
        }

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
