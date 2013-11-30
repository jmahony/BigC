package com.joshmahony.bigc;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.jsoup.HttpStatusException;
import org.jsoup.Jsoup;
import org.jsoup.nodes.Document;
import redis.clients.jedis.Jedis;
import redis.clients.jedis.JedisPool;

import java.io.IOException;
import java.net.URL;


/**
 * Created with IntelliJ IDEA.
 * User: joshmahony
 * Date: 13/11/2013
 * Time: 19:14
 * To change this template use File | Settings | File Templates.
 */
class Crawler implements Runnable {

    private CrawlerDispatcher crawlerDispatcher;
    private JedisPool pool;

    final Logger logger = LogManager.getLogger(Crawler.class);

    final static String userAgent = "BigC";
    final static String referrer = "http://wwwjoshmahony.com/";


    public Crawler(CrawlerDispatcher cd, JedisPool jp) {
        crawlerDispatcher = cd;
        pool = jp;
    }

    @Override
    public void run() {
        while(true) {
            try {

                Jedis connection = pool.getResource();
                Document d = null;

                logger.info("Fetching next URL to crawl...");
                URL urlToCrawl = crawlerDispatcher.getNextURL();

                if (urlToCrawl == null) continue;

                try {
                    logger.info("Attempting to crawl " + urlToCrawl.toString());
                    d = Jsoup.connect(urlToCrawl.toString()).userAgent(userAgent).referrer(referrer).header("Accept", "text/html").get();
                    crawlerDispatcher.addURLsToQueue(d);

                    connection.set(urlToCrawl.toString(), d.toString());
                } catch(HttpStatusException e) {
                    //TODO: Do something here
                } catch (IOException e) {
                    e.printStackTrace();  //To change body of catch statement use File | Settings | File Templates.
                }

                pool.returnResource(connection);
                Thread.sleep(crawlerDispatcher.DEFAULT_CRAWL_RATE);
            } catch (InterruptedException e) {
                e.printStackTrace();  //To change body of catch statement use File | Settings | File Templates.
            }
        }
    }

}
