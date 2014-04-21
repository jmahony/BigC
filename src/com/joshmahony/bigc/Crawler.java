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
 * The main crawling loop, multiple instances can be run in separate threads,
 * it simply goes through the queue downloading and store the HTML.
 */
@Log4j2
class Crawler implements Runnable {

    /**
     * Stores a reference the crawler dispatch object
     */
    private static CrawlerDispatcher crawlerDispatcher = null;

    /**
     * The frontier to crawl
     */
    private static CrawlQueue crawlQueue = null;

    /**
     * Stores a reference to the HTML Store
     */
    private static HTMLStore store = null;

    /**
     * Flag for the while loop
     */
    private boolean running = true;

    /**
     *
     * Contructor
     *
     * @param cd the crawler dispatcher
     * @param cq the crawl queue (frontier)
     * @param s the HTML store
     */
    public Crawler(CrawlerDispatcher cd, CrawlQueue cq, HTMLStore s) {

        if (crawlerDispatcher == null) {

            crawlerDispatcher = cd;

            crawlQueue = cq;

            store = s;

        }

    }

    /**
     *
     * Terminates the crawl loop
     *
     */
    public void terminate() {

        running = false;

    }

    /**
     *
     * Crawl the web
     *
     */
    @Override
    public void run() {

        log.info("I'm alive!!");

        while(running) {

            try {

                log.debug("Fetching next URL to crawl...");

                URL urlToCrawl = crawlQueue.getNextURL();

                log.info("Attempting to crawl " + urlToCrawl.toString());

                Connection.Response res = Jsoup.connect(urlToCrawl.toString())
                        .userAgent(C.USER_AGENT)
                        .referrer(C.REFERRER)
                        .header("Accept", "text/html")
                        .execute();

                Document d = res.parse();

                HashSet<URL> urls = URLExtractor.extract(d);

                crawlQueue.enqueueURLs(urls);

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

            } catch (URLDiscoveredException e) {

                log.info("Attempting to add URL that has already been discovered, possbile bloom collision");

                continue;

            }

            waitFor(C.DEFAULT_CRAWL_RATE);

        }

    }

    /**
     *
     * Makes the loop wait for a bit for the queue to fill up with some more
     * domains
     *
     * @param milliseconds how to long to wait in milliseconds
     */
    private void waitFor(long milliseconds) {

        try {

            Thread.sleep(milliseconds);

        } catch (InterruptedException e) {

            log.warn(e.getMessage());

        }

    }

}
