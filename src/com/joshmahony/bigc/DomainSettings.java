package com.joshmahony.bigc;

import lombok.Getter;
import lombok.Setter;
import lombok.extern.log4j.Log4j2;
import org.json.simple.JSONObject;

import java.net.URL;

/**
 * Parses and holds the settings for a domain
 */
@Log4j2
public class DomainSettings {

    /**
     * The crawl rate
     */
    private @Getter @Setter long crawlRate;

    /**
     *
     * Constructor
     *
     * @param domain the domain to extract the settings from
     * @param object the actual JSON settings
     */
    public DomainSettings(URL domain, JSONObject object) {

        if (object.containsKey("crawlRate")) {

            long cr = (Long) object.get("crawlRate");

            if (cr < C.MIN_CRAWL_RATE) {

                log.warn("Crawl rate for " + domain +
                        " is too low, setting to min crawl rate: " +
                        C.MIN_CRAWL_RATE);

                crawlRate = C.MIN_CRAWL_RATE;

            } else {

                crawlRate = cr;

            }

        } else {

            crawlRate = C.DEFAULT_CRAWL_RATE;

        }

    }

    public DomainSettings(URL domain) {

        this(domain, new JSONObject());

    }

}
