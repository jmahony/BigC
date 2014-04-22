package com.joshmahony.bigc;

import lombok.extern.log4j.Log4j2;

import java.net.UnknownHostException;

/**
 * Dispatches the crawler threads
 */
@Log4j2
public class CrawlerDispatcher {

    /**
     * Stores a reference to each crawler
     */
    private Crawler[] crawlers;

    /**
     * The crawl queue
     */
    private CrawlQueue crawlQueue;

    /**
     *
     * Main
     *
     * @param args arguments
     */
    public static void main(String args[]) {

        try {

            new CrawlerDispatcher(args);

        } catch (UnknownHostException e) {

            log.fatal("Could not connect to MongoDB");

        }

    }

    /**
     *
     * Constructor
     *
     * @param args arguments
     */
    public CrawlerDispatcher(String args[]) throws UnknownHostException {

        log.info("Initialising MongoDB connection connection... ");

        crawlers  = new Crawler[C.NUM_THREADS];

        log.info("Crawler dispatcher launching");

        crawlQueue = new CrawlQueue(args[0]);

        dispatch();

    }

    /**
     *
     * Shutdown the crawler
     *
     */
    public void terminate() {

        for (int i = 0; i < 0; i++) crawlers[i].terminate();

    }

    /**
     *
     * Create all of the crawler threads
     *
     */
    public void dispatch() {

        HTMLStore store = HTMLStore.getInstance();

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
