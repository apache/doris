// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements.  See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership.  The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License.  You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.

package org.apache.doris.http.action;

import org.apache.doris.PaloFe;
import org.apache.doris.http.ActionController;
import org.apache.doris.http.BaseRequest;
import org.apache.doris.http.BaseResponse;
import org.apache.doris.http.IllegalArgException;

import com.google.common.base.Strings;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.io.File;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Calendar;
import java.util.Date;
import java.util.GregorianCalendar;
import java.util.Locale;
import java.util.TimeZone;
import java.util.regex.Pattern;

import javax.activation.MimetypesFileTypeMap;

import io.netty.handler.codec.http.HttpHeaderNames;
import io.netty.handler.codec.http.HttpMethod;
import io.netty.handler.codec.http.HttpResponseStatus;

/**
 * A simple handler that serves incoming HTTP requests to send their respective
 * HTTP responses.  It also implements {@code 'If-Modified-Since'} header to
 * take advantage of browser cache, as described in
 * <a href="http://tools.ietf.org/html/rfc2616#section-14.25">RFC 2616</a>.
 *
 * <h3>How Browser Caching Works</h3>
 *
 * Web browser caching works with HTTP headers as illustrated by the following
 * sample:
 * <ol>
 * <li>Request #1 returns the content of {@code /file1.txt}.</li>
 * <li>Contents of {@code /file1.txt} is cached by the browser.</li>
 * <li>Request #2 for {@code /file1.txt} does return the contents of the
 *     file again. Rather, a 304 Not Modified is returned. This tells the
 *     browser to use the contents stored in its cache.</li>
 * <li>The server knows the file has not been modified because the
 *     {@code If-Modified-Since} date is the same as the file's last
 *     modified date.</li>
 * </ol>
 *
 * <pre>
 * Request #1 Headers
 * ===================
 * GET /file1.txt HTTP/1.1
 *
 * Response #1 Headers
 * ===================
 * HTTP/1.1 200 OK
 * Date:               Tue, 01 Mar 2011 22:44:26 GMT
 * Last-Modified:      Wed, 30 Jun 2010 21:36:48 GMT
 * Expires:            Tue, 01 Mar 2012 22:44:26 GMT
 * Cache-Control:      private, max-age=31536000
 *
 * Request #2 Headers
 * ===================
 * GET /file1.txt HTTP/1.1
 * If-Modified-Since:  Wed, 30 Jun 2010 21:36:48 GMT
 *
 * Response #2 Headers
 * ===================
 * HTTP/1.1 304 Not Modified
 * Date:               Tue, 01 Mar 2011 22:44:28 GMT
 *
 * </pre>
 *
 * We use parameter named 'res' to specify the static resource path, it relative to the
 * root path of http server.
 */
public class StaticResourceAction extends WebBaseAction {
    private static final Logger LOG = LogManager.getLogger(StaticResourceAction.class);

    public static final String HTTP_DATE_FORMAT = "EEE, dd MMM yyyy HH:mm:ss zzz";
    public static final String HTTP_DATE_GMT_TIMEZONE = "GMT";
    public static final int HTTP_CACHE_SECONDS = 2592000;

    public MimetypesFileTypeMap mimeTypesMap;
    public String rootDir;

    public StaticResourceAction(ActionController controller, String rootDir) {
        super(controller);
        mimeTypesMap = new MimetypesFileTypeMap();
        mimeTypesMap.addMimeTypes("text/html html htm");
        // According to RFC 4329(http://tools.ietf.org/html/rfc4329#section-7.2), the MIME type of
        // javascript script is 'application/javascript'
        mimeTypesMap.addMimeTypes("application/javascript js");
        // mimeTypesMap.addMimeTypes("text/javascript js");
        mimeTypesMap.addMimeTypes("text/css css");

        this.rootDir = rootDir;
    }

    public static void registerAction(ActionController controller) throws IllegalArgException {
        String httpDir = PaloFe.DORIS_HOME_DIR + "/webroot";
        StaticResourceAction action = new StaticResourceAction(controller, httpDir + "/static");
        controller.registerHandler(HttpMethod.GET, "/static/js", action);
        controller.registerHandler(HttpMethod.GET, "/static/css", action);
        controller.registerHandler(HttpMethod.GET, "/static", action);
        controller.registerHandler(HttpMethod.GET, "/static/resource", action);
        controller.registerHandler(HttpMethod.GET, "/static/images", action);
        controller.registerHandler(HttpMethod.GET, "/static/fonts", action);

        StaticResourceAction action2 = new StaticResourceAction(controller, "webroot");
        controller.registerHandler(HttpMethod.GET, "/static_test", action2);
    }

    @Override
    public boolean needAdmin() {
        return false;
    }

    @Override
    public void executeGet(BaseRequest request, BaseResponse response) {
        String resourcePath = request.getSingleParameter("res");
        if (Strings.isNullOrEmpty(resourcePath)) {
            LOG.error("Wrong request without 'res' parameter. url: {}",
                    request.getRequest().uri());
            writeResponse(request, response, HttpResponseStatus.BAD_REQUEST);
            return;
        }
        resourcePath = sanitizePath(resourcePath);
        if (resourcePath == null) {
            LOG.error("Close this request because of risk factor in 'res' parameter.url: {}",
                    request.getRequest().uri());
            writeResponse(request, response, HttpResponseStatus.FORBIDDEN);
            return;
        }

        String resourceAbsolutePath = rootDir + File.separator + resourcePath;
        File resFile = new File(resourceAbsolutePath);
        LOG.debug("resAbsolutePath: {}", resourceAbsolutePath);

        if (!resFile.exists() || resFile.isHidden() || resFile.isDirectory()) {
            LOG.error("Request with wrong path. url: {}", request.getRequest().uri());
            writeResponse(request, response, HttpResponseStatus.NOT_FOUND);
            return;
        }
        if (!resFile.isFile()) {
            LOG.error("Wrong request: not normal file. url: {}", request.getRequest().uri());
            writeResponse(request, response, HttpResponseStatus.FORBIDDEN);
            return;
        }

        // Cache validation
        String ifModifiedSince = request.getRequest()
                .headers().get(HttpHeaderNames.IF_MODIFIED_SINCE.toString());
        if (!Strings.isNullOrEmpty(ifModifiedSince)) {
            SimpleDateFormat dateFormatter = new SimpleDateFormat(HTTP_DATE_FORMAT, Locale.US);
            Date ifModifiedSinceDate;
            try {
                ifModifiedSinceDate = dateFormatter.parse(ifModifiedSince);
                // Only compare up to the second because the datetime format we send to the client
                // does not have milliseconds
                long ifModifiedSinceDateSeconds = ifModifiedSinceDate.getTime() / 1000;
                long fileLastModifiedSeconds = resFile.lastModified() / 1000;
                if (ifModifiedSinceDateSeconds == fileLastModifiedSeconds) {
                    writeResponse(request, response, HttpResponseStatus.NOT_MODIFIED);
                    return;
                }
            } catch (ParseException e) {
                LOG.error("Fail to analyse IF_MODIFIED_SINCE header: ", e);
                writeResponse(request, response, HttpResponseStatus.BAD_REQUEST);
                return;
            }
        }

        response.updateHeader(HttpHeaderNames.CONTENT_TYPE.toString(), getContentType(resourceAbsolutePath));
        setDateAndCacheHeaders(response, resFile);

        writeObjectResponse(request, response, HttpResponseStatus.OK, resFile, resFile.getName(), false);
    }



    // Gets the content type header for the HTTP Response
    private String getContentType(String filename) {
        return mimeTypesMap.getContentType(filename);
    }

    private void setDateAndCacheHeaders(BaseResponse response, File fileToCache) {
        SimpleDateFormat dateFormatter = new SimpleDateFormat(HTTP_DATE_FORMAT, Locale.US);
        dateFormatter.setTimeZone(TimeZone.getTimeZone(HTTP_DATE_GMT_TIMEZONE));

        // Date header
        Calendar time = new GregorianCalendar();
        response.updateHeader(HttpHeaderNames.DATE.toString(), dateFormatter.format(time.getTime()));

        // Add cache headers
        time.add(Calendar.SECOND, HTTP_CACHE_SECONDS);
        response.updateHeader(HttpHeaderNames.EXPIRES.toString(), dateFormatter.format(time.getTime()));
        response.updateHeader(HttpHeaderNames.CACHE_CONTROL.toString(), "private, max-age=" + HTTP_CACHE_SECONDS);
        response.updateHeader(HttpHeaderNames.LAST_MODIFIED.toString(),
                dateFormatter.format(new Date(fileToCache.lastModified())));
    }

    private static final Pattern INSECURE_URI = Pattern.compile(".*[<>&\"].*");

    private String sanitizePath(String path) {
        if (Strings.isNullOrEmpty(path)) {
            return null;
        }
        // Convert file separators
        String newPath = path.replace('/', File.separatorChar);

        // Simplistic dumb security check.
        if (newPath.contains(File.separator + '.')
                || newPath.contains('.' + File.separator)
                || newPath.charAt(0) == '.'
                || newPath.charAt(newPath.length() - 1) == '.'
                || INSECURE_URI.matcher(newPath).matches()) {
            return null;
        }

        return newPath;
    }
}
