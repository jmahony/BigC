package com.joshmahony.bigc;

import lombok.extern.log4j.Log4j2;
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
@Log4j2
class Crawler implements Runnable {

    /**
     * Stores a reference the crawler dispatch object
     */
    private static CrawlerDispatcher crawlerDispatcher = null;

    /**
     * Stores a reference to the crawl queue
     */
    private static CrawlQueue crawlQueue;

    /**
     * Stores a reference to the HTML Store
     */
    private static HTMLStore store;

    /**
     * Flag for the while loop
     */
    private boolean running = true;

    /**
     *
     * @param cd
     * @param cq
     * @param s
     */
    public Crawler(CrawlerDispatcher cd, CrawlQueue cq, HTMLStore s) {

        if (crawlerDispatcher == null) {

            crawlerDispatcher = cd;

            crawlQueue = cq;

            store = s;

        }

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

        log.info("I'm alive!!");

        while(running) {
            try {

                log.debug("Fetching next URL to crawl...");

                URL urlToCrawl = crawlQueue.getNextURL();

                log.info("Attempting to crawl " + urlToCrawl.toString());

                Connection.Response res = Jsoup.connect(urlToCrawl.toString()).userAgent(C.USER_AGENT).referrer(C.REFERRER).header("Accept", "text/html").execute();

                Document d = res.parse();

                HashSet<URL> urls = HTMLUtils.extractURLs(d);

                crawlQueue.enqueueURLs(urls);

                log.info("Storing HTML " + urlToCrawl.toString());

                store.store(urlToCrawl, d);


            } catch(HttpStatusException e) {

                log.warn(e.getMessage());

            } catch (IOException e) {

                log.error(e.getMessage());

            } catch (NoAvailableDomainsException e) {

                log.info(e.getMessage());

                waitFor(C.DEFAULT_CRAWL_RATE);

                continue;

            } catch (CrawlQueueEmptyException e) {

                log.info(e.getMessage());

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

            log.warn(e.getMessage());

        }

    }

}
