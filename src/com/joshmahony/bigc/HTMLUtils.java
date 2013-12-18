package com.joshmahony.bigc;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.jsoup.nodes.Document;
import org.jsoup.nodes.Element;
import org.jsoup.select.Elements;

import java.net.MalformedURLException;
import java.net.URL;
import java.util.HashSet;

/**
 * Created by joshmahony on 18/12/2013.
 */
public class HTMLUtils {


    private static Logger logger = LogManager.getLogger(CrawlQueue.class.getName());

    /**
         * Extracts the URLs from a Jsoup document and returns then as a HashMap with the URL as key and URL object as
         * value
         *
         * @param d
         * @return
         */
        public static HashSet<URL> extractURLs(Document d) {

            // Create a new hashmap to store the URLs, using a hashmap as we dont need multiple items for each URL
            HashSet<URL> urls = new HashSet();

            // Get the links out of the document
            Elements links = d.select("a[href");

            // Go through each link adding them to the hashmap
            for (Element link : links) {

                try {

                    URL url = new URL(link.attr("abs:href"));

                    urls.add(url);

                } catch (MalformedURLException e) {

                    // Doesnt really matter if the URL is malformed, just log that we have encountered one
                    logger.debug("Malformed URL: " + e.getMessage());

                }

            }

            return urls;

        }

}
