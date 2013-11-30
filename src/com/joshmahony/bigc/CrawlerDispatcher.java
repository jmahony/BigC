package com.joshmahony.bigc;

import com.google.common.base.Charsets;
import com.google.common.io.Files;

import com.sun.net.httpserver.Headers;
import com.sun.net.httpserver.HttpExchange;
import com.sun.net.httpserver.HttpHandler;
import com.sun.net.httpserver.HttpServer;


import org.apache.logging.log4j.*;

import org.json.simple.JSONArray;
import org.jsoup.nodes.Document;
import org.jsoup.nodes.Element;
import org.jsoup.select.Elements;
import redis.clients.jedis.Jedis;
import redis.clients.jedis.JedisPool;
import redis.clients.jedis.JedisPoolConfig;

import java.io.*;
import java.net.*;

import java.util.*;
import java.util.concurrent.ConcurrentLinkedQueue;

import org.json.simple.JSONObject;
import org.json.simple.parser.JSONParser;


/**
 * Created with IntelliJ IDEA.
 * User: joshmahony
 * Date: 13/11/2013
 * Time: 19:04
 * To change this template use File | Settings | File Templates.
 */
public class CrawlerDispatcher {

    private final int NUM_THREADS       = 1;
    public final int DEFAULT_CRAWL_RATE = 10000;
    public final int MIN_CRAWL_RATE     = 5000;

    private JedisPool pool;
    private ConcurrentLinkedQueue<URL> URLsToCrawl = new ConcurrentLinkedQueue<URL>();

    Logger logger = LogManager.getLogger(CrawlerDispatcher.class.getName());
    private HttpServer server;

    private JSONObject politeTimes;

    public static void main(String args[]) {
        new CrawlerDispatcher(args);
    }

    public CrawlerDispatcher(String args[]) {

        /*try {
            server = HttpServer.create(new InetSocketAddress(8002), 0);
            server.createContext("/test", new MyHandler(this));
            server.setExecutor(null); // creates a default executor
            server.start();
        } catch (IOException e) {
            e.printStackTrace();  //To change body of catch statement use File | Settings | File Templates.
        }*/

        logger.info("Crawler dispatcher launching");

        logger.info("Initialising Redis connection pool... ");
        pool = new JedisPool(new JedisPoolConfig(), "localhost");
        logger.info("Done");

        politeTimes = loadJSONFromFile(args[0]);
        validatePoliteTimes();

        loadURLList(args[1]);

        dispatch();

    }

    static class MyHandler implements HttpHandler {

        CrawlerDispatcher crawlerDispatcher;

        public MyHandler(CrawlerDispatcher cd) {
            crawlerDispatcher = cd;
        }

        public void handle(HttpExchange t) throws IOException {

            StringBuffer sb = new StringBuffer();
            JSONArray ja = new JSONArray();
            for(URL url : crawlerDispatcher.URLsToCrawl) {
                JSONObject jo = new JSONObject();
                jo.put("domain", url.toString());
                ja.add(jo);
            }

            String response = ja.toJSONString();

            Headers h = t.getResponseHeaders();
            h.set("Content-Type","text/jsonp");
            h.set("Access-Control-Allow-Origin","*");
            t.sendResponseHeaders(200, response.length());

            OutputStream os = t.getResponseBody();
            os.write(response.getBytes());
            os.close();
        }
    }

    public void dispatch() {

        for (int i = 0; i < NUM_THREADS; i++) {
            (new Thread(new Crawler(this, pool))).start();
        }

    }

    public void addURLsToQueue(Document d) {

        try {
            Elements links = d.select("a[href]");
            for(Element link : links) {
                URL url = new URL(link.attr("abs:href"));
                if (!URLsToCrawl.contains(url)) {
                    URLsToCrawl.add(url);
                }
            }
        } catch (Exception e) {
            logger.error("Failed " + e.getMessage());
            System.exit(0);
        }

    }

    public URL getNextURL() {


        if (URLsToCrawl.isEmpty()) {
            logger.info("URLsToCrawl is empty, exiting program");
            System.exit(0);
        }

        URL url = URLsToCrawl.poll();

        if (!alreadyCrawled(url.toString())) {
            return url;
        }

        return getNextURL();


    }

    public boolean alreadyCrawled(String url) {

        Jedis connection = pool.getResource();

        return connection.exists(url);

    }

    public boolean isAlreadyCrawled(String url) {

        Jedis connection = pool.getResource();
        return connection.exists(url);

    }

    private void loadURLList(String path) {

        try {
            String s = fileToString(path);
            String[] sa = s.split("\n");
            for (int i = 0; i < sa.length; i++) {
                URL url = new URL(sa[i]);
                URLsToCrawl.add(url);
            }
        } catch (Exception e) {
            logger.error("Failed " + e.getMessage());
            System.exit(0);
        }

    }

    private void saveURLList(String path) {

        ObjectOutputStream oos = null;
        try {
            oos = new ObjectOutputStream(new BufferedOutputStream(new FileOutputStream(path)));
            oos.writeObject(URLsToCrawl);
            oos.close();
        } catch (IOException e) {
            e.printStackTrace();  //To change body of catch statement use File | Settings | File Templates.
        }

    }

    private JSONObject loadJSONFromFile(String path) {

        logger.info("Loading JSON from " + path + "... ");
        String JSONString = null;
        Object o = null;

        try {
            JSONString = fileToString(path);
        } catch (IOException e) {
            logger.error("Failed " + e.getMessage());
            System.exit(0);
        }

        logger.info("Parsing JSON from " + path + "... ");

        JSONParser parser = new JSONParser();

        try {
            o = parser.parse(JSONString);

            if (!(o instanceof JSONObject)) {
                throw new Exception("Please provide a single JSON object");
            }

        } catch (Exception e) {
            logger.error("Failed " + e.getMessage());
            System.exit(0);
        }

        return (JSONObject) o;

    }

    private void validatePoliteTimes() {

        logger.info("Validating polite times... ");

        Iterator i = politeTimes.entrySet().iterator();

        while (i.hasNext()) {
            Map.Entry p = (Map.Entry) i.next();
            JSONObject domain = (JSONObject) p.getValue();

            if (!domain.containsKey("crawlRate")) {
                logger.warn("Crawl rate not set for " + p.getKey() + " too low, setting crawl rate to " + MIN_CRAWL_RATE);
                domain.put("crawlRate", DEFAULT_CRAWL_RATE);
            }

            if (Integer.parseInt(domain.get("crawlRate").toString()) < MIN_CRAWL_RATE) {
                logger.warn("Minimum crawl rate is " + MIN_CRAWL_RATE + ", " + p.getKey() + " is " + domain.get("crawlRate") + ". Increasing to " + MIN_CRAWL_RATE);
                domain.put("crawlRate", MIN_CRAWL_RATE);
            }
        }
    }

    private static String fileToString(String path) throws IOException {
        String content = Files.toString(new File(path), Charsets.UTF_8);

        return content;
    }

}