package com.joshmahony.bigc;

import com.mongodb.*;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.json.simple.JSONObject;

import java.net.MalformedURLException;
import java.net.URL;
import java.net.UnknownHostException;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;

/**
 * Created by joshmahony on 15/12/2013.
 */
public class CrawlQueue {

    /**
     * Store the connection pool to MongoDB
     */
    public MongoClient pool;

    /**
     *
     */
    private static HashSet<String> domainWhiteList;

    /**
     * Creates an instance of log4j
     */
    private final Logger logger;

    /**
     * This stores a list of domains we can crawl TODO: extend this to include a cached version of robots.txt
     */
    ConcurrentHashMap<String, Domain> domainList;

    /**
     *
     * @param seedPath
     */
    public CrawlQueue(String seedPath) {

        logger = LogManager.getLogger(CrawlQueue.class.getName());

        domainWhiteList = new HashSet();

        domainList = new ConcurrentHashMap();

        initMongoConnection();

        initPoliteTimes(seedPath);

    }

    /**
     * Creare a connection to mongo
     */
    private void initMongoConnection() {

        logger.info("Initialising MongoDB connection pool... ");

        try {

            pool = new MongoClient(C.MONGO_HOST, C.MONGO_PORT);

        } catch (UnknownHostException e) {

            logger.fatal("Could not connect to MongoDB server");

            System.exit(-1);

        }

    }

    /**
     * Load the polite times JSON file
     * @param path
     */
    private void initPoliteTimes(String path) {

        // Load the polite times JSON file
        JSONObject times = FileLoader.fileToJSON(path);

        Iterator i = times.entrySet().iterator();

        // Iterate over each domain in the polite times
        while (i.hasNext()) {

            Map.Entry entry = (Map.Entry) i.next();

            try {

                Domain d = new Domain("http://" + entry.getKey().toString(), pool, entry.getValue());

                addToDomainWhiteList(d);

            } catch (MalformedURLException e) {

                logger.fatal("Malformed URL " + entry.getKey().toString());

                System.exit(0);

            }

        }

    }

    /**
     *
     * @param d
     */
    private void addToDomainList(Domain d) {

        logger.info("Adding to domain list: " + d.getDomain());

        domainList.put(d.getDomain(), d);

    }

    /**
     *
     * @param d
     */
    private void addToDomainWhiteList(Domain d) {

        logger.info("Adding to domain white list: " + d.getDomain());

        domainWhiteList.add(d.getDomain());

        addToDomainList(d);

    }

    /**
     * Enqueues a list of domains for the given domain
     * @param urls
     */
    public synchronized void enqueueURLs(HashSet<URL> urls) {

        HashMap<URL, HashSet<String>> map = new HashMap();

        for (URL url : urls) {

            try {

                URL u = new URL(url.getProtocol() + "://" + url.getHost());

                if (!map.containsKey(u)) map.put(u, new HashSet());

                map.get(u).add(url.getFile() + (url.getQuery() != null ? url.getQuery() : ""));

            } catch (MalformedURLException e) {

                logger.warn("Malformed URL");

            }

        }

        Iterator itr = map.entrySet().iterator();

        while (itr.hasNext()) {

            // Get the next domain
            Map.Entry entry = (Map.Entry) itr.next();

            URL domain = (URL) entry.getKey();

            try {

                if (!domainList.containsKey(domain.getHost())) {

                    domainList.put(((URL) entry.getKey()).getHost(), new Domain(domain.toString(), pool));
                }

                domainList.get(domain.getHost()).enqueueURLs((HashSet<String>) entry.getValue());

            } catch (MalformedURLException e) {

                logger.warn("Malformed URL " + entry.getKey().toString());

            }

        }

    }

    /**
     * Returns a the next URL for an available domain
     * @return URL || null
     */
    public synchronized URL getNextURL() throws CrawlQueueEmptyException, NoAvailableDomainsException {

        Domain d = getNextDomain();

        return d.getNextURL();

    }

    /**
     * Returns a domain that is availble to crawl
     * TODO: Potential problem : domains at the end of the list may never get crawled, maybe re implement as some kind of
     * Queue system
     * @return
     */
    private synchronized Domain getNextDomain() throws NoAvailableDomainsException {

        // Get the iterator for the entries in the domainList hashmap
        Iterator itr = domainList.entrySet().iterator();

        Domain domain = null;

        long current = System.currentTimeMillis();

        // Iterate over each entry in domainList
        while (itr.hasNext()) {

            // Get the next domain
            Map.Entry domainEntry = (Map.Entry) itr.next();

            Domain d = (Domain) domainEntry.getValue();

            if (!domainWhiteList.contains(d.getDomain()) && C.ONLY_CRAWL_WHITELIST) continue;

            if (d.isEmpty) continue;

            // Get the last time the domain was crawled
            long lastCrawlTimestamp = d.getLastCrawlTime();

            // If 0, the domain hasn't been crawled on this session
            if (lastCrawlTimestamp == 0) {

                domain = d;

                break;

            }

            long difference = current - lastCrawlTimestamp;

            // Check if we're allowed to crawl the domain
            if (difference > C.DEFAULT_CRAWL_RATE) {

                domain = d;

                break;

            }

        }

        if (domain == null) throw new NoAvailableDomainsException("No domains available at this time");

        return domain;

    }

}