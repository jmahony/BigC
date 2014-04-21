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
 * Created by joshmahony on 15/12/2013.
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
     * @param seedPath
     */
    public CrawlQueue(String seedPath) {

        domainWhiteList = new HashSet();

        domainList = new ConcurrentHashMap();

        queueIterator = null;

        initPoliteTimes(seedPath);

    }

    /**
     * Load the polite times JSON file
     * @param path
     */
    private void initPoliteTimes(String path) {

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
     * @param d
     */
    private void addToDomainList(Domain d) {

        log.debug("Adding to domain list: " + d.getDomain());

        domainList.put(d.getDomain(), d);

    }

    /**
     *
     * @param d
     */
    private void addToDomainWhiteList(Domain d) {

        log.debug("Adding to domain white list: " + d.getDomain());

        domainWhiteList.add(d.getDomain());

        addToDomainList(d);

    }

    /**
     * Enqueues a list of domains for the given domain
     * @param urls
     */
    public synchronized void enqueueURLs(HashSet<URL> urls) {

        HashMap<URL, HashSet<String>> map = new HashMap();

        for (URL url : urls) {

            try {

                URL u = new URL(url.getProtocol() + "://" + url.getHost());

                if (!map.containsKey(u)) map.put(u, new HashSet());

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
     * Returns a the next URL for an available domain
     * @return URL || null
     */
    public synchronized URL getNextURL() throws CrawlQueueEmptyException, NoAvailableDomainsException {

        Domain d = getNextDomain();

        return d.getNextURL();

    }

    /**
     * Returns a domain that is availble to crawl
     * TODO: Potential problem : domains at the end of the list may never get crawled, maybe re implement as some kind of
     * Queue system
     * @return
     */
    private synchronized Domain getNextDomain() throws NoAvailableDomainsException {

        // If the iterator hasn't been initilised, or it has no more elements, bring the iterator back to the start
        // by creating a new instance. We keep a reference so all domains are attempted at some point.
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

        if (domain == null) throw new NoAvailableDomainsException("No domains available at this time");

        return domain;

    }

}
