package com.joshmahony.bigc;

import lombok.extern.log4j.Log4j2;
import org.jsoup.nodes.Document;
import org.jsoup.nodes.Element;
import org.jsoup.select.Elements;

import java.net.MalformedURLException;
import java.net.URL;
import java.util.HashSet;

/**
 * Created by joshmahony on 18/12/2013.
 */
@Log4j2
public class URLExtractor {

    /**
     *
     * Extracts the URLs from a JSoup document and returns then as a HashMap
     * with the URL as key and URL object as value
     *
     * @param d the document to get the URLs out of
     * @return the set of URLs
     */
    public static HashSet<URL> extract(Document d) {

        // Create a new hash map to store the URLs, using a hash map as we don't
        // need multiple items for each URL
        HashSet<URL> urls = new HashSet();

        // Get the links out of the document
        Elements links = d.select("a[href");

        // Go through each link adding them to the hash map
        for (Element link : links) {

            try {

                URL url = new URL(link.attr("abs:href"));

                urls.add(url);

            } catch (MalformedURLException e) {

                // Doesn't really matter if the URL is malformed, just log
                // that we have encountered one
                log.debug("Malformed URL: " + e.getMessage());

            }

        }

        return urls;

    }

}
