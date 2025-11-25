package org.apache.doris.cdcclient.utils;

import org.apache.http.client.config.RequestConfig;
import org.apache.http.impl.NoConnectionReuseStrategy;
import org.apache.http.impl.client.CloseableHttpClient;
import org.apache.http.impl.client.DefaultRedirectStrategy;
import org.apache.http.impl.client.HttpClients;
import org.apache.http.protocol.HttpRequestExecutor;
import org.apache.http.protocol.RequestContent;

public class HttpUtil {
    private static int connectTimeout = 30 * 1000;
    private static int waitForContinueTimeout = 60 * 1000;
    private static int socketTimeout = 10 * 60 * 1000; // stream load timeout 10 min

    public static CloseableHttpClient getHttpClient() {
        return HttpClients.custom()
                // default timeout 3s, maybe report 307 error when fe busy
                .setRequestExecutor(new HttpRequestExecutor(waitForContinueTimeout))
                .setRedirectStrategy(
                        new DefaultRedirectStrategy() {
                            @Override
                            protected boolean isRedirectable(String method) {
                                return true;
                            }
                        })
                .setRetryHandler((exception, executionCount, context) -> false)
                .setConnectionReuseStrategy(NoConnectionReuseStrategy.INSTANCE)
                .setDefaultRequestConfig(
                        RequestConfig.custom()
                                .setConnectTimeout(connectTimeout)
                                .setConnectionRequestTimeout(connectTimeout)
                                .setSocketTimeout(socketTimeout)
                                .build())
                .addInterceptorLast(new RequestContent(true))
                .build();
    }
}
