package com.joshmahony.bigc;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.jsoup.Connection;
import org.jsoup.HttpStatusException;
import org.jsoup.Jsoup;
import org.jsoup.nodes.Document;

import java.io.IOException;
import java.net.URL;
import java.util.HashSet;


/**
 * Created with IntelliJ IDEA.
 * User: joshmahony
 * Date: 13/11/2013
 * Time: 19:14
 * To change this template use File | Settings | File Templates.
 */
class Crawler implements Runnable {

    /**
     * Stores a reference the crawler dispatch object
     */
    private CrawlerDispatcher crawlerDispatcher;

    /**
     * Stores a reference to the crawl queue
     */
    private CrawlQueue crawlQueue;

    /**
     * Stores an instance of log4j
     */
    private final Logger logger;

    /**
     * Flag for the while loop
     */
    private boolean running = true;

    /**
     * Stores a reference to the HTML Store
     */
    private HTMLStore store;

    /**
     *
     * @param cd
     * @param cq
     * @param s
     */
    public Crawler(CrawlerDispatcher cd, CrawlQueue cq, HTMLStore s) {

        logger = LogManager.getLogger(Crawler.class);

        crawlerDispatcher = cd;

        crawlQueue = cq;

        store = s;

    }

    /**
     * Terminates the crawl loop
     */
    public void terminate() {

        running = false;

    }

    /**
     * Crawl the web
     */
    @Override
    public void run() {

        logger.info("I'm alive!!");

        while(running) {
            try {

                logger.debug("Fetching next URL to crawl...");

                URL urlToCrawl = crawlQueue.getNextURL();

                logger.info("Attempting to crawl " + urlToCrawl.toString());

                Connection.Response res = Jsoup.connect(urlToCrawl.toString()).userAgent(C.USER_AGENT).referrer(C.REFERRER).header("Accept", "text/html").execute();

                Document d = res.parse();

                HashSet<URL> urls = HTMLUtils.extractURLs(d);

                crawlQueue.enqueueURLs(urls);

                logger.info("Storing HTML " + urlToCrawl.toString());

                store.store(urlToCrawl, d);


            } catch(HttpStatusException e) {

                logger.warn(e.getMessage());

            } catch (IOException e) {

                logger.error(e.getMessage());

            } catch (NoAvailableDomainsException e) {

                logger.info(e.getMessage());

                waitFor(C.DEFAULT_CRAWL_RATE);

                continue;

            } catch (CrawlQueueEmptyException e) {

                logger.info(e.getMessage());

                waitFor(C.DEFAULT_CRAWL_RATE);

                continue;

            }

            waitFor(C.DEFAULT_CRAWL_RATE);

        }

    }

    /**
     * Suspend the loop
     * @param millis
     */
    private void waitFor(long millis) {

        try {

            Thread.sleep(millis);

        } catch (InterruptedException e) {

            logger.warn(e.getMessage());

        }

    }

}
