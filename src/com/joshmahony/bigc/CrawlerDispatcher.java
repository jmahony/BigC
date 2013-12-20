package com.joshmahony.bigc;

import org.apache.logging.log4j.*;

/**
 * User: Josh Mahony (jm426@uni.brighton.ac.uk)
 * Date: 13/11/2013
 * Time: 19:04
 */
public class CrawlerDispatcher {

    /**
     * Stores a reference to each crawler
     */
    public Crawler[] crawlers;

    /**
     *
     */
    private CrawlQueue crawlQueue;

    /**
     * Creates an instance of log4j
     */
    private final Logger logger;

    /**
     *
     */
    private HTMLStore store;

    /**
     *
     * @param args
     */
    public static void main(String args[]) {

        new CrawlerDispatcher(args);

    }

    /**
     *
     * @param args
     */
    public CrawlerDispatcher(String args[]) {

        logger = LogManager.getLogger(CrawlerDispatcher.class.getName());

        crawlers  = new Crawler[C.NUM_THREADS];

        logger.info("Crawler dispatcher launching");

        crawlQueue = new CrawlQueue(args[0]);

        store = new HTMLStore();

        dispatch();

    }

    /**
     * Shutdown the crawler
     */
    public void terminate() {

        for (int i = 0; i < 0; i++) crawlers[i].terminate();

    }

    /**
     * Create all of the crawler threads
     */
    public void dispatch() {

        for (int i = 0; i < C.NUM_THREADS; i++) {

            // Create a crawler and pass it a reference to the crawler dispatcher and the connection connection
            Crawler c = new Crawler(this, crawlQueue, store);

            // Store a reference to the crawler thread, so we can perform operations on it later
            crawlers[i] = c;

            // Start the crawler in a new thread
            (new Thread(c)).start();

        }

    }

}