package com.joshmahony.bigc;

import lombok.extern.log4j.Log4j2;
import org.json.simple.JSONObject;

import java.net.URL;

/**
 * Created by joshmahony on 18/12/2013.
 */
@Log4j2
public class SettingsParser {

    /**
     *
     */
    private long crawlRate;

    /**
     *
     * @param _domain
     * @param _object
     */
    public SettingsParser(URL _domain, JSONObject _object) {


        if (_object.containsKey("crawlRate")) {

            long cr = (Long) _object.get("crawlRate");

            if (cr < C.MIN_CRAWL_RATE) {

                log.warn("Crawl rate for " + _domain + " is too low, setting to min crawl rate: " + C.MIN_CRAWL_RATE);

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
