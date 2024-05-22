package org.apache.doris.cdcloader.mysql.utils;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.net.HttpURLConnection;
import java.net.URL;

public class LoaderUtil {
    private static final Logger LOG = LoggerFactory.getLogger(LoaderUtil.class);
    public static boolean tryHttpConnection(String host) {
        try {
            if (LOG.isDebugEnabled()) {
                LOG.debug("try to connect host {}", host);
            }
            host = "http://" + host;
            URL url = new URL(host);
            HttpURLConnection co = (HttpURLConnection) url.openConnection();
            co.setConnectTimeout(60000);
            co.connect();
            co.disconnect();
            return true;
        } catch (Exception ex) {
            LOG.warn("Failed to connect to host:{}", host, ex);
            return false;
        }
    }
}
