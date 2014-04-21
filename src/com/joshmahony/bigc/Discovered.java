package com.joshmahony.bigc;

import lombok.extern.log4j.Log4j2;
import orestes.bloomfilter.redis.BloomFilterRedis;

import java.net.URL;

/**
 * Bloom filter to check if a URL has already been discovered
 */
@Log4j2
public class Discovered {

    /**
     * The bloom filter
     */
    private static final BloomFilterRedis<String> BLOOM_FILTER = new BloomFilterRedis<>(
            C.REDIS_HOST,
            C.REDIS_PORT,
            C.BLOOM_FILTER_N,
            C.BLOOM_FILTER_P);

    /**
     *
     * Mark a URL as being discovered
     *
     * @param url the URL
     */
    public synchronized static void insert(URL url) {

        long start = System.currentTimeMillis();

        BLOOM_FILTER.add(url.toString());

        long end = System.currentTimeMillis();

        log.debug("Bloom filter insert took: " + (end - start) + "ms");

    }

    /**
     *
     * Returns whether or not the URL may have been seen
     *
     * @param url the URL to check
     * @return true = maybe seen, false = haven't seen
     */
    public synchronized static boolean check(URL url) {

        long start = System.currentTimeMillis();

        boolean contains = BLOOM_FILTER.contains(url.toString());

        long end = System.currentTimeMillis();

        log.debug("Bloom filter check took: " + (end - start) + "ms");

        return contains;

    }

}
