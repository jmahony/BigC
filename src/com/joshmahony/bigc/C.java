package com.joshmahony.bigc;

/**
 * Created by joshmahony on 16/12/2013.
 */
public class C {

    /**
     * User agent the crawler shows as
     */
    public final static String USER_AGENT = "BigC";

    /**
     * Crawlers referrer
     */
    public final static String REFERRER = "http://wwwjoshmahony.com/";

    /**
     * The name of the collection of domain queue
     */
    public static final String CRAWL_QUEUE_COLLECTION = "crawlQueue";

    /**
     * The name of the collection of domain queue
     */
    public static final String HTML_STORE_COLLECTION = "htmlStore";

    /**
     * The host name of the MongoDB server
     */
    public static final String MONGO_HOST = "localhost";

    /**
     * The port of the MongoDB server
     */
    public static final int MONGO_PORT = 27017;

    /**
     * The name of the collection of domain queue
     */
    public static final String MONGO_DATABASE = "bigc";

    /**
     * Number of threads to dispatch
     */
    public static final int NUM_THREADS = 5;

    /**
     * The default rate each crawler will visit a domain
     */
    public static final int DEFAULT_CRAWL_RATE = 10000;

    /**
     * The minimum rate the crawler is allowed to visit a domain
     */
    public static final int MIN_CRAWL_RATE = 5000;

    /**
     *
     */
    public static final boolean ONLY_CRAWL_WHITELIST = false;

}
