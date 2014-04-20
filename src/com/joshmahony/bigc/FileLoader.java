package com.joshmahony.bigc;

import com.google.common.base.Charsets;
import com.google.common.io.Files;
import lombok.extern.log4j.Log4j2;
import org.json.simple.JSONObject;
import org.json.simple.parser.JSONParser;

import java.io.File;
import java.io.IOException;

/**
 * Created by joshmahony on 15/12/2013.
 */
@Log4j2
public class FileLoader {

    public static String fileToString(String path) throws IOException {
        String content = Files.toString(new File(path), Charsets.UTF_8);

        return content;
    }

    /**
     * Takes a path and turns it into a JSON object
     * @param path
     * @return
     */
    public static JSONObject fileToJSON(String path) {

        log.info("Loading JSON from " + path + "... ");
        String JSONString = null;
        Object o = null;

        try {
            JSONString = FileLoader.fileToString(path);
        } catch (IOException e) {
            log.error("Failed " + e.getMessage());
            System.exit(0);
        }

        log.info("Parsing JSON from " + path + "... ");

        JSONParser parser = new JSONParser();

        try {
            o = parser.parse(JSONString);

            if (!(o instanceof JSONObject)) {
                throw new Exception("Please provide a single JSON object");
            }

        } catch (Exception e) {
            log.error("Failed " + e.getMessage());
            System.exit(0);
        }

        return (JSONObject) o;

    }

}
