package com.joshmahony.bigc;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.jsoup.Connection;
import org.jsoup.HttpStatusException;
import org.jsoup.Jsoup;
import org.jsoup.nodes.Document;
import redis.clients.jedis.exceptions.JedisConnectionException;

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

    private CrawlerDispatcher crawlerDispatcher;
    private CrawlQueue crawlQueue;

    final Logger logger = LogManager.getLogger(Crawler.class);

    private boolean running = true;

    private HTMLStore store;


    public Crawler(CrawlerDispatcher cd, CrawlQueue cq, HTMLStore s) {
        crawlerDispatcher = cd;
        crawlQueue = cq;
        store = s;
    }

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

                logger.info("Storing HTML" + urlToCrawl.toString());

                store.store(urlToCrawl, d);

            } catch (JedisConnectionException e) {

                logger.error(e.getMessage());

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

    private void waitFor(long millis) {

        try {

            Thread.sleep(millis);

        } catch (InterruptedException e) {

            logger.warn(e.getMessage());

        }

    }


}
