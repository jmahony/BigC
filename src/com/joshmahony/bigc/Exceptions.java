package com.joshmahony.bigc;

class CrawlQueueEmptyException extends Exception {

    CrawlQueueEmptyException(String m) {
        super(m);
    }

}
class NoAvailableDomainsException extends Exception {

    NoAvailableDomainsException(String m) {
        super(m);
    }

}

