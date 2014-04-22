package com.joshmahony.bigc;

import com.mongodb.*;
import crawlercommons.fetcher.http.BaseHttpFetcher;
import crawlercommons.fetcher.http.UserAgent;
import crawlercommons.robots.BaseRobotRules;
import crawlercommons.robots.RobotUtils;
import crawlercommons.robots.SimpleRobotRulesParser;
import lombok.Getter;
import lombok.Setter;
import lombok.extern.log4j.Log4j2;

import java.net.MalformedURLException;
import java.net.URL;
import java.util.HashSet;

/**
 * Domain holds a queue of URLs within that need to be crawled.
 */
@Log4j2
public class Domain {

    /**
     * The unix timestamp of the last time the crawler crawled this domain
     */
    private @Getter @Setter long lastCrawlTime;

    /**
     * The settings for the domain, this includes the crawl rate
     */
    private DomainSettings settings;

    /**
     * The actual domain
     */
    private URL domain;

    /**
     * Stores whether the queue is empty or not
     * TODO: Do something when the queue is empty, possibly recrawl?
     */
    private @Getter boolean isEmpty;

    /**
     * Store the robots.txt rules
     */
    private BaseRobotRules rules;

    /**
     * Whether or not the robots.txt file has been fetched
     */
    private boolean hasRobots;

    /**
     *
     * Takes in a domain name, a database connection and a settings object
     *
     * @param domain the domains url
     * @param settings the settings for the domain
     * @throws MalformedURLException if invalid domain
     */
    public Domain(URL domain, DomainSettings settings) throws MalformedURLException {

        lastCrawlTime = 0;

        hasRobots = false;

        this.domain = domain;

        this.settings = settings;

        createDomainQueue();

    }

    /**
     *
     * Takes the domain name and creates a default
     *
     * @param domain the domains url
     * @throws MalformedURLException
     */
    public Domain(URL domain) throws MalformedURLException {

        this(domain, new DomainSettings(domain));

    }

    /**
     *
     * Creates a domain queue in the database
     *
     */
    private void createDomainQueue() {

        // Make sure the domain doesn't already have a document
        if (domainQueueExists()){

            log.debug(getDomain() + " Already has domain queue in database");

            return;

        }

        log.debug("Creating new domain queue for " + getDomain() + " in database");

        DBCollection collection = Mongo.getCollection(C.CRAWL_QUEUE_COLLECTION);

        // Create the domainQueue
        BasicDBObject domainQueue = new BasicDBObject();

        domainQueue.append("domain", getDomain());

        //TODO: Add query string?
        domainQueue.append("queue", new String[] {"/"});

        //Insert queue into database
        collection.insert(domainQueue);

    }

    /**
     *
     * Enqueues a list of domains for the domain
     *
     * @param urls URLs to queue
     */
    public void enqueueURLs(HashSet<String> urls) {

        cleanURLs(urls);

        // Get the crawl queue collection
        DBCollection collection = Mongo.getCollection(C.CRAWL_QUEUE_COLLECTION);

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
     *
     * Removes entries from a set of urls according to robots.txt and whether
     * they have already been crawled
     *
     * @param urls URLs to clean
     * @return the cleans set of URLs
     */
    private HashSet<String> cleanURLs(HashSet<String> urls) {

        if (!hasRobots) initRobots();

        // Don't enqueue URL if its disallowed in robots.txt
        if (rules != null) {

            HashSet<String> remove = new HashSet<String>();

            for(String url : urls) {

                URL u = makeURLFromPath(url);

                boolean robotsAllowed = rules.isAllowed(u.toString());

                boolean alreadyCrawled = Discovered.check(u);

                if (!robotsAllowed || alreadyCrawled || url.length() == 0) {

                    remove.add(url);

                }

            }

            urls.removeAll(remove);

        }

        return urls;

    }

    /**
     *
     * Returns a the next URL for an available domain
     *
     * @return the next URL
     */
    public synchronized URL getNextURL() throws CrawlQueueEmptyException {

        DBCollection collection = Mongo.getCollection(C.CRAWL_QUEUE_COLLECTION);

        // Fetch the document, this will retrieve the whole document and
        // then pop one domainName from the queue
        DBObject result = collection.findAndModify(
                new BasicDBObject("domain", getDomain()), // Find this
                new BasicDBObject("$pop", new BasicDBObject("queue", -1)) // Do this
        );

        // Get the domains queue
        BasicDBList queue = (BasicDBList) result.get("queue");

        // Check the queue isn't empty
        if (queue.isEmpty()) {

            isEmpty = true;

            throw new CrawlQueueEmptyException("The crawl queue for " + getDomain() + " is empty!");

        }

        setLastCrawlTime(System.currentTimeMillis());

        return makeURLFromPath(queue.get(0).toString());

    }

    /**
     *
     * Rebuilds a url from the path
     *
     * @param path the path
     * @return the URL
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
     *
     * Is this going to hang with every new domain created?!?!?!
     * TODO: run this in its own thread or create a job to fetch robots.txt OR!!
     * TODO: make the crawler fetch it on its first crawl.
     *
     */
    private void initRobots() {

        if (hasRobots) return;

        // Only get robots.txt if the domain is in the white list
        if (!CrawlQueue.getDomainWhiteList().contains(getDomain())) return;

        log.info("Fetching robots.txt for " + getDomain());

        UserAgent ua = new UserAgent("BigC", "jm426@uni.brighton.ac.uk", "joshmahony.com");

        BaseHttpFetcher bf = RobotUtils.createFetcher(ua, 1);

        SimpleRobotRulesParser parser = new SimpleRobotRulesParser();

        try {

           rules = RobotUtils.getRobotRules(bf, parser, new URL(domain.toString() + "/robots.txt"));

           hasRobots = true;

        } catch (MalformedURLException e) {

            e.printStackTrace();
        }

    }

    /**
     *
     * Checks to see if the domain already has a document in the database
     *
     * @return true if the domain has a queue in the database
     */
    private synchronized boolean domainQueueExists() {

        DBCollection collection = Mongo.getCollection(C.CRAWL_QUEUE_COLLECTION);

        DBObject query = new BasicDBObject("domain", getDomain());

        DBCursor cursor = collection.find(query).limit(1);

        return cursor.size() > 0;

    }

    /**
     *
     * Get the crawl rate for a domain
     *
     * @return the crawl rate in milliseconds
     */
    public long getCrawlRate() {

        return settings.getCrawlRate();

    }

    /**
     *
     * Returns the domain
     *
     * @return the domain
     */
    public String getDomain() {

        return domain.getHost();

    }

}
