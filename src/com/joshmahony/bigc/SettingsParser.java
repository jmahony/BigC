package com.joshmahony.bigc;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.json.simple.JSONObject;

import java.net.URL;

/**
 * Created by joshmahony on 18/12/2013.
 */
public class SettingsParser {


    /**
     *
     */
    private long crawlRate;

    /**
     *
     */
    private final Logger logger;

    /**
     *
     * @param _domain
     * @param _object
     */
    public SettingsParser(URL _domain, JSONObject _object) {

        logger = LogManager.getLogger(SettingsParser.class.getName());


        if (_object.containsKey("crawlRate")) {

            long cr = (Long) _object.get("crawlRate");

            if (cr < C.MIN_CRAWL_RATE) {

                logger.warn("Crawl rate for " + _domain + " is too low, setting to min crawl rate: " + C.MIN_CRAWL_RATE);

                crawlRate = C.MIN_CRAWL_RATE;

            } else {

                crawlRate = cr;

            }

        } else {

            crawlRate = C.DEFAULT_CRAWL_RATE;

        }

    }

    /**
     *
     * @return
     */
    public long getCrawlRate() {
        return crawlRate;
    }


}
