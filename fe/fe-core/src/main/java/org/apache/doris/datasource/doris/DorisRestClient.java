package org.apache.doris.datasource.doris;

import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import lombok.Data;
import okhttp3.Credentials;
import okhttp3.OkHttpClient;
import okhttp3.Request;
import okhttp3.Response;
import org.apache.doris.common.util.JsonUtil;
import org.apache.http.HttpHeaders;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.logging.log4j.util.Strings;

import javax.net.ssl.HostnameVerifier;
import javax.net.ssl.SSLContext;
import javax.net.ssl.SSLSession;
import javax.net.ssl.SSLSocketFactory;
import javax.net.ssl.TrustManager;
import javax.net.ssl.X509TrustManager;
import java.io.IOException;
import java.security.SecureRandom;
import java.security.cert.CertificateException;
import java.security.cert.X509Certificate;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.TimeUnit;

public class DorisRestClient {
    private static final Logger LOG = LogManager.getLogger(DorisRestClient.class);

    private static final OkHttpClient networkClient = new OkHttpClient
        .Builder().readTimeout(10, TimeUnit.SECONDS).build();

    private static OkHttpClient sslNetworkClient;
    private final Request.Builder builder;
    private final List<String> feNodes;
    private String currentNode;
    private int currentNodeIndex = 0;
    private final boolean httpSslEnable;

    /**
     * For DorisTable.
     **/
    public DorisRestClient(List<String> feNodes, String authUser, String authPassword, boolean httpSslEnable) {
        this.feNodes = feNodes;
        this.builder = new Request.Builder();
        if (!Strings.isEmpty(authUser)) {
            this.builder.addHeader(HttpHeaders.AUTHORIZATION, Credentials.basic(authUser, Strings.isEmpty(authPassword) ? "" : authPassword));
        }
        this.currentNode = feNodes.get(currentNodeIndex);
        this.httpSslEnable = httpSslEnable;
    }

    public List<String> getDatabaseNameList() {
        DorisApiResponse databasesResponse = parseResponse(execute("api/meta/namespaces/default_cluster/databases"),
            "get doris databases error");

        List<String> databases = new ArrayList<>();
        if (successResponse(databasesResponse)) {
            ArrayNode jsonNodes = JsonUtil.parseArray(databasesResponse.getData());
            jsonNodes.forEach(json -> {
                databases.add(json.textValue());
            });
        }

        return databases;
    }

    public List<String> getTablesNameList(String dbName) {
        DorisApiResponse tablesResponse = parseResponse(execute("api/meta/namespaces/default_cluster/databases/"+dbName+"/tables"),
            "get doris tables error");

        List<String> tables = new ArrayList<>();
        if (successResponse(tablesResponse)) {
            ArrayNode jsonNodes = JsonUtil.parseArray(tablesResponse.getData());
            jsonNodes.forEach(json -> {
                tables.add(json.textValue());
            });
        }

        return tables;
    }

    public boolean isTableExist(String dbName, String tblName) {
        DorisApiResponse tableSchema = parseResponse(execute("api/" + dbName+"/" + tblName + "/_schema"),
            "get doris table schema error");
        return successResponse(tableSchema);
    }

    public boolean health() {
        DorisApiResponse healthResponse  = parseResponse(execute("api/health"),
            "get doris table schema error");
        if (successResponse(healthResponse)) {
            ObjectNode objectNode = JsonUtil.parseObject(healthResponse.getData());

            Integer aliveBeNum = JsonUtil.safeGetAsInt(objectNode, "online_backend_num");
            return aliveBeNum!=null && aliveBeNum>0;
        }
        return false;
    }

    public String getJsonColumns(String dbName, String tableName) {
        DorisApiResponse tableSchemaResponse = parseResponse(execute("api/"+dbName+"/"+tableName+"/_schema"),
            "get doris table schema error");
        return tableSchemaResponse.getData();
    }

    private boolean successResponse(DorisApiResponse response) {
        return (response.getCode() != null && response.getCode() == 0)
            || "success".equals(response.msg)
            || "OK".equals(response.msg);
    }

    private void selectNextNode() {
        currentNodeIndex++;
        currentNodeIndex = currentNodeIndex%feNodes.size();
        currentNode = feNodes.get(currentNodeIndex);
    }

    private DorisApiResponse parseResponse(String response, String errMsg) {
        if (response == null) {
            throw new RuntimeException(errMsg);
        }

        ObjectNode objectNode = JsonUtil.parseObject(response);

        return new DorisApiResponse(
            objectNode.path(DorisApiResponse.MSG).asText(null),
            JsonUtil.safeGetAsInt(objectNode, DorisApiResponse.CODE),
            JsonUtil.convertNodeToString(objectNode.path(DorisApiResponse.DATA)),
            JsonUtil.safeGetAsInt(objectNode, DorisApiResponse.COUNT)
        );
    }

    @Data
    public static class DorisApiResponse {
        public static final String MSG = "msg";
        public static final String CODE = "code";
        public static final String DATA = "data";
        public static final String COUNT = "count";

        private String msg;
        private Integer code;
        private String data;
        private Integer count;

        public DorisApiResponse() {}

        public DorisApiResponse(String msg, Integer code, String data, Integer count) {
            this.msg = msg;
            this.code = code;
            this.data = data;
            this.count = count;
        }
    }

    public OkHttpClient getClient() {
        if (httpSslEnable) {
            return getOrCreateSslNetworkClient();
        }
        return networkClient;
    }

    /**
     * init ssl networkClient use lazy way
     **/
    private synchronized OkHttpClient getOrCreateSslNetworkClient() {
        if (sslNetworkClient == null) {
            sslNetworkClient = new OkHttpClient.Builder().readTimeout(10, TimeUnit.SECONDS)
                .sslSocketFactory(createSSLSocketFactory(), new TrustAllCerts())
                .hostnameVerifier(new TrustAllHostnameVerifier()).build();
        }
        return sslNetworkClient;
    }

    private Response executeResponse(OkHttpClient httpClient, String path) throws IOException {
        currentNode = currentNode.trim();
        if (!(currentNode.startsWith("http://") || currentNode.startsWith("https://"))) {
            currentNode = "http://" + currentNode;
        }
        if (!currentNode.endsWith("/")) {
            currentNode = currentNode + "/";
        }
        Request request = builder.get().url(currentNode + path).build();
        if (LOG.isInfoEnabled()) {
            LOG.info("doris rest client request URL: {}", request.url().toString());
        }
        return httpClient.newCall(request).execute();
    }

    /**
     * execute request for specific path，it will try again nodes.length times if it fails
     *
     * @param path the path must not leading with '/'
     * @return response
     */
    private String execute(String path) {
        // try 3 times for every node
        int retrySize = feNodes.size() * 3;
        RuntimeException scratchExceptionForThrow = null;
        OkHttpClient httpClient;
        if (httpSslEnable) {
            httpClient = getOrCreateSslNetworkClient();
        } else {
            httpClient = networkClient;
        }
        for (int i = 0; i < retrySize; i++) {
            // maybe should add HTTP schema to the address
            // actually, at this time we can only process http protocol
            // NOTE. currentNode may have some spaces.
            // User may set a config like described below:
            // hosts: "http://192.168.0.1:8200, http://192.168.0.2:8200"
            // then currentNode will be "http://192.168.0.1:8200", " http://192.168.0.2:8200"
            if (LOG.isTraceEnabled()) {
                LOG.trace("doris rest client request URL: {}", currentNode + "/" + path);
            }
            try (Response response = executeResponse(httpClient, path)) {
                if (response.isSuccessful()) {
                    return response.body().string();
                } else {
                    LOG.warn("request response code: {}, body: {}", response.code(), response.message());
                    scratchExceptionForThrow = new RuntimeException(response.message());
                }
            } catch (IOException e) {
                LOG.warn("request node [{}] [{}] failures {}, try next nodes", currentNode, path, e);
                scratchExceptionForThrow = new RuntimeException(e.getMessage());
            }
            selectNextNode();
        }
        LOG.warn("try all nodes [{}], no other nodes left", feNodes);
        if (scratchExceptionForThrow != null) {
            throw scratchExceptionForThrow;
        }
        return null;
    }

    /**
     * support https
     **/
    private static class TrustAllCerts implements X509TrustManager {
        public void checkClientTrusted(X509Certificate[] chain, String authType) throws CertificateException {
        }

        public void checkServerTrusted(X509Certificate[] chain, String authType) throws CertificateException {
        }

        public X509Certificate[] getAcceptedIssuers() {
            return new X509Certificate[0];
        }
    }

    private static class TrustAllHostnameVerifier implements HostnameVerifier {
        public boolean verify(String hostname, SSLSession session) {
            return true;
        }
    }

    private static SSLSocketFactory createSSLSocketFactory() {
        SSLSocketFactory ssfFactory;
        try {
            SSLContext sc = SSLContext.getInstance("TLS");
            sc.init(null, new TrustManager[] {new TrustAllCerts()}, new SecureRandom());
            ssfFactory = sc.getSocketFactory();
        } catch (Exception e) {
            throw new RuntimeException("Errors happens when create ssl socket");
        }
        return ssfFactory;
    }
}
