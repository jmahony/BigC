package com.joshmahony.bigc;

import lombok.Getter;
import lombok.extern.log4j.Log4j2;
import org.json.simple.JSONObject;

import java.net.MalformedURLException;
import java.net.URL;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

/**
 * The main frontier of the crawler, it is responsible for only returning
 * URLs that it would be polite to crawl
 */
@Log4j2
public class CrawlQueue {

    /**
     * Stores a list of domains we're allowed to crawl
     */
    private @Getter static HashSet<String> domainWhiteList;

    /**
     * Used to store an instance of the queue iterator, so we can resume the search after a URL is returned.
     * Otherwise we would start at the beginning each time.
     */
    private Iterator queueIterator;

    /**
     * This stores a list of domains we can crawl TODO: extend this to include a cached version of robots.txt
     */
    private ConcurrentHashMap<String, Domain> domainList;

    /**
     *
     * Constructor
     *
     * @param seedPath The path of the seed list
     */
    public CrawlQueue(String seedPath) {

        domainWhiteList = new HashSet();

        domainList = new ConcurrentHashMap();

        queueIterator = null;

        initSeedList(seedPath);

    }

    /**
     *
     * Enqueues a list of URLs into the right domain
     *
     * @param urls the URLs to enqueue
     */
    public synchronized void enqueueURLs(HashSet<URL> urls) {

        HashMap<URL, HashSet<String>> map = new HashMap<>();

        for (URL url : urls) {

            try {

                URL u = new URL(url.getProtocol() + "://" + url.getHost());

                if (!map.containsKey(u)) map.put(u, new HashSet<>());

                map.get(u).add(url.getFile() + (url.getQuery() != null ? url.getQuery() : ""));

            } catch (MalformedURLException e) {

                log.warn("Malformed URL");

            }

        }

        for (Object o : map.entrySet()) {

            // Get the next domain
            Map.Entry entry = (Map.Entry) o;

            URL domain = (URL) entry.getKey();

            try {

                if (domainWhiteList.contains(domain.getHost())) {

                    if (!domainList.containsKey(domain.getHost())) {

                        domainList.put(((URL) entry.getKey()).getHost(), new Domain(domain));

                    }

                    domainList.get(domain.getHost()).enqueueURLs((HashSet<String>) entry.getValue());

                }

            } catch (MalformedURLException e) {

                log.warn("Malformed URL " + entry.getKey().toString());

            }

        }

    }

    /**
     *
     * Returns a the next URL for an available domain
     *
     * @return the next URL we are allowed to crawl
     */
    public synchronized URL getNextURL() throws CrawlQueueEmptyException, NoAvailableDomainsException {

        Domain d = getNextDomain();

        return d.getNextURL();

    }

    /**
     *
     * Load the polite times JSON file
     *
     * @param path path to the JSON
     */
    private void initSeedList(String path) {

        // Load the polite times JSON file
        JSONObject times = FileLoader.fileToJSON(path);

        // Iterate over each domain in the polite times
        for (Object o : times.entrySet()) {

            Map.Entry entry = (Map.Entry) o;

            try {

                URL url = new URL("http://" + entry.getKey().toString());

                DomainSettings settings = new DomainSettings(url, (JSONObject) entry.getValue());

                Domain d = new Domain(url, settings);

                addToDomainWhiteList(d);

            } catch (MalformedURLException e) {

                log.fatal("Malformed URL " + entry.getKey().toString());

                System.exit(0);

            }

        }

    }

    /**
     *
     * Adds a domain to the list to crawl
     *
     * @param domain the domain object
     */
    private void addToDomainList(Domain domain) {

        log.debug("Adding to domain list: " + domain.getDomain());

        domainList.put(domain.getDomain(), domain);

    }

    /**
     *
     * Adds a domain to the white list
     *
     * @param domain the domain object
     */
    private void addToDomainWhiteList(Domain domain) {

        log.debug("Adding to domain white list: " + domain.getDomain());

        domainWhiteList.add(domain.getDomain());

        addToDomainList(domain);

    }

    /**
     *
     * Returns a domain that is available to crawl

     * @return the domain that we are allowed to crawl
     */
    private synchronized Domain getNextDomain() throws NoAvailableDomainsException {

        // If the iterator hasn't been initialised, or it has no more elements,
        // bring the iterator back to the start by creating a new instance. We
        // keep a reference so all domains are attempted at some point.
        if (queueIterator == null || !queueIterator.hasNext()) {

            queueIterator = domainList.entrySet().iterator();

        }

        Domain domain = null;

        // Iterate over each entry in domainList
        while (queueIterator.hasNext()) {

            // Get the next domain
            Map.Entry domainEntry = (Map.Entry) queueIterator.next();

            Domain d = (Domain) domainEntry.getValue();

            long current = System.currentTimeMillis();

            if (!domainWhiteList.contains(d.getDomain()) && C.ONLY_CRAWL_WHITELIST) continue;

            if (d.isEmpty()) continue;

            // Get the last time the domain was crawled
            long lastCrawlTimestamp = d.getLastCrawlTime();

            // If 0, the domain hasn't been crawled on this session
            if (lastCrawlTimestamp == 0) {

                domain = d;

                break;

            }

            long difference = current - lastCrawlTimestamp;

            // Check if we're allowed to crawl the domain
            if (difference > d.getCrawlRate()) {

                domain = d;

                break;

            }

        }

        if (domain == null)
            throw new NoAvailableDomainsException("No domains available");

        return domain;

    }

}
