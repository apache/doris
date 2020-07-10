package org.apache.doris.http.util;

import static org.springframework.http.HttpHeaders.CONNECTION;

import com.google.common.base.Strings;

import java.io.BufferedReader;
import java.io.IOException;

import javax.servlet.http.HttpServletRequest;

public class HttpUtil {
    public static boolean isKeepAlive(HttpServletRequest request ) {
        if(!request.getHeader(CONNECTION).equals("close") &&
                (request.getProtocol().equals("") ||
                        request.getHeader(CONNECTION).equals("keep-alive"))){
            return true;
        }
        return false;
    }

    public static boolean isSslEnable(HttpServletRequest request){
        String url = request.getRequestURL().toString();
        if(!Strings.isNullOrEmpty(url) && url.startsWith("https")){
            return true;
        }
        return false;
    }

    public static String getBody(HttpServletRequest request) {
        StringBuffer data = new StringBuffer();
        String line = null;
        BufferedReader reader = null;
        try {
            reader = request.getReader();
            while (null != (line = reader.readLine()))
                data.append(new String(line.getBytes("utf-8")));
        } catch (IOException e) {
        } finally {
        }
        return data.toString();
    }
}
