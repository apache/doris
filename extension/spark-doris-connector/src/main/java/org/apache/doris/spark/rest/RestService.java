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

package org.apache.doris.spark.rest;

import static org.apache.doris.spark.cfg.ConfigurationOptions.DORIS_FENODES;
import static org.apache.doris.spark.cfg.ConfigurationOptions.DORIS_FILTER_QUERY;
import static org.apache.doris.spark.cfg.ConfigurationOptions.DORIS_READ_FIELD;
import static org.apache.doris.spark.cfg.ConfigurationOptions.DORIS_REQUEST_AUTH_PASSWORD;
import static org.apache.doris.spark.cfg.ConfigurationOptions.DORIS_REQUEST_AUTH_USER;
import static org.apache.doris.spark.cfg.ConfigurationOptions.DORIS_TABLET_SIZE;
import static org.apache.doris.spark.cfg.ConfigurationOptions.DORIS_TABLET_SIZE_DEFAULT;
import static org.apache.doris.spark.cfg.ConfigurationOptions.DORIS_TABLET_SIZE_MIN;
import static org.apache.doris.spark.cfg.ConfigurationOptions.DORIS_TABLE_IDENTIFIER;
import static org.apache.doris.spark.util.ErrorMessages.CONNECT_FAILED_MESSAGE;
import static org.apache.doris.spark.util.ErrorMessages.ILLEGAL_ARGUMENT_MESSAGE;
import static org.apache.doris.spark.util.ErrorMessages.PARSE_NUMBER_FAILED_MESSAGE;
import static org.apache.doris.spark.util.ErrorMessages.SHOULD_NOT_HAPPEN_MESSAGE;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.PrintWriter;
import java.io.Serializable;
import java.net.HttpURLConnection;
import java.net.URL;
import java.nio.charset.StandardCharsets;
import java.util.Map;
import java.util.HashMap;
import java.util.Base64;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.ArrayList;
import java.util.Set;
import java.util.HashSet;
import java.util.stream.Collectors;

import org.apache.commons.io.IOUtils;
import org.apache.commons.lang3.StringUtils;
import org.apache.doris.spark.cfg.ConfigurationOptions;
import org.apache.doris.spark.cfg.Settings;
import org.apache.doris.spark.cfg.SparkSettings;
import org.apache.doris.spark.exception.ConnectedFailedException;
import org.apache.doris.spark.exception.DorisException;
import org.apache.doris.spark.exception.IllegalArgumentException;
import org.apache.doris.spark.exception.ShouldNeverHappenException;
import org.apache.doris.spark.rest.models.Backend;
import org.apache.doris.spark.rest.models.BackendRow;
import org.apache.doris.spark.rest.models.BackendV2;
import org.apache.doris.spark.rest.models.QueryPlan;
import org.apache.doris.spark.rest.models.Schema;
import org.apache.doris.spark.rest.models.Tablet;
import org.apache.http.HttpStatus;
import org.apache.http.client.config.RequestConfig;
import org.apache.http.client.methods.HttpGet;
import org.apache.http.client.methods.HttpPost;
import org.apache.http.client.methods.HttpRequestBase;
import org.apache.http.entity.StringEntity;
import org.codehaus.jackson.JsonParseException;
import org.codehaus.jackson.map.JsonMappingException;
import org.codehaus.jackson.map.ObjectMapper;
import org.slf4j.Logger;

import com.google.common.annotations.VisibleForTesting;

/**
 * Service for communicate with Doris FE.
 */
public class RestService implements Serializable {
    public final static int REST_RESPONSE_STATUS_OK = 200;
    private static final String API_PREFIX = "/api";
    private static final String SCHEMA = "_schema";
    private static final String QUERY_PLAN = "_query_plan";
    @Deprecated
    private static final String BACKENDS = "/rest/v1/system?path=//backends";
    private static final String BACKENDS_V2 = "/api/backends?is_alive=true";

    /**
     * send request to Doris FE and get response json string.
     * @param cfg configuration of request
     * @param request {@link HttpRequestBase} real request
     * @param logger {@link Logger}
     * @return Doris FE response in json string
     * @throws ConnectedFailedException throw when cannot connect to Doris FE
     */
    private static String send(Settings cfg, HttpRequestBase request, Logger logger) throws
            ConnectedFailedException {
        int connectTimeout = cfg.getIntegerProperty(ConfigurationOptions.DORIS_REQUEST_CONNECT_TIMEOUT_MS,
                ConfigurationOptions.DORIS_REQUEST_CONNECT_TIMEOUT_MS_DEFAULT);
        int socketTimeout = cfg.getIntegerProperty(ConfigurationOptions.DORIS_REQUEST_READ_TIMEOUT_MS,
                ConfigurationOptions.DORIS_REQUEST_READ_TIMEOUT_MS_DEFAULT);
        int retries = cfg.getIntegerProperty(ConfigurationOptions.DORIS_REQUEST_RETRIES,
                ConfigurationOptions.DORIS_REQUEST_RETRIES_DEFAULT);
        logger.trace("connect timeout set to '{}'. socket timeout set to '{}'. retries set to '{}'.",
                connectTimeout, socketTimeout, retries);

        RequestConfig requestConfig = RequestConfig.custom()
                .setConnectTimeout(connectTimeout)
                .setSocketTimeout(socketTimeout)
                .build();

        request.setConfig(requestConfig);
        String user = cfg.getProperty(DORIS_REQUEST_AUTH_USER, "");
        String password = cfg.getProperty(DORIS_REQUEST_AUTH_PASSWORD, "");
        logger.info("Send request to Doris FE '{}' with user '{}'.", request.getURI(), user);
        IOException ex = null;
        int statusCode = -1;

        for (int attempt = 0; attempt < retries; attempt++) {
            logger.debug("Attempt {} to request {}.", attempt, request.getURI());
            try {
                String response;
                if (request instanceof HttpGet){
                    response = getConnectionGet(request.getURI().toString(), user, password,logger);
                } else {
                    response = getConnectionPost(request,user, password,logger);
                }
                if (response == null) {
                    logger.warn("Failed to get response from Doris FE {}, http code is {}",
                            request.getURI(), statusCode);
                    continue;
                }
                logger.trace("Success get response from Doris FE: {}, response is: {}.",
                        request.getURI(), response);
                ObjectMapper mapper = new ObjectMapper();
                Map map = mapper.readValue(response, Map.class);
                //Handle the problem of inconsistent data format returned by http v1 and v2
                if (map.containsKey("code") && map.containsKey("msg")) {
                    Object data = map.get("data");
                    return mapper.writeValueAsString(data);
                } else {
                    return response;
                }
            } catch (IOException e) {
                ex = e;
                logger.warn(CONNECT_FAILED_MESSAGE, request.getURI(), e);
            }
        }

        logger.error(CONNECT_FAILED_MESSAGE, request.getURI(), ex);
        throw new ConnectedFailedException(request.getURI().toString(), statusCode, ex);
    }

    private static String getConnectionGet(String request,String user, String passwd,Logger logger) throws IOException {
        URL realUrl = new URL(request);
        // open connection
        HttpURLConnection connection = (HttpURLConnection)realUrl.openConnection();
        String authEncoding = Base64.getEncoder().encodeToString(String.format("%s:%s", user, passwd).getBytes(StandardCharsets.UTF_8));
        connection.setRequestProperty("Authorization", "Basic " + authEncoding);

        connection.connect();
        return parseResponse(connection,logger);
    }

    private static String parseResponse(HttpURLConnection connection,Logger logger) throws IOException {
        if (connection.getResponseCode() != HttpStatus.SC_OK) {
            logger.warn("Failed to get response from Doris  {}, http code is {}",
                    connection.getURL(), connection.getResponseCode());
            throw new IOException("Failed to get response from Doris");
        }
        StringBuilder result = new StringBuilder("");
        BufferedReader in = new BufferedReader(new InputStreamReader(connection.getInputStream(), "utf-8"));
        String line;
        while ((line = in.readLine()) != null) {
            result.append(line);
        }
        if (in != null) {
            in.close();
        }
        return result.toString();
    }

    private static String getConnectionPost(HttpRequestBase request,String user, String passwd,Logger logger) throws IOException {
        URL url = new URL(request.getURI().toString());
        HttpURLConnection conn = (HttpURLConnection) url.openConnection();
        conn.setInstanceFollowRedirects(false);
        conn.setRequestMethod(request.getMethod());
        String authEncoding = Base64.getEncoder().encodeToString(String.format("%s:%s", user, passwd).getBytes(StandardCharsets.UTF_8));
        conn.setRequestProperty("Authorization", "Basic " + authEncoding);
        InputStream content = ((HttpPost)request).getEntity().getContent();
        String res = IOUtils.toString(content);
        conn.setDoOutput(true);
        conn.setDoInput(true);
        PrintWriter out = new PrintWriter(conn.getOutputStream());
        // send request params
        out.print(res);
        // flush
        out.flush();
        // read response
        return parseResponse(conn,logger);
    }
    /**
     * parse table identifier to array.
     * @param tableIdentifier table identifier string
     * @param logger {@link Logger}
     * @return first element is db name, second element is table name
     * @throws IllegalArgumentException table identifier is illegal
     */
    @VisibleForTesting
    static String[] parseIdentifier(String tableIdentifier, Logger logger) throws IllegalArgumentException {
        logger.trace("Parse identifier '{}'.", tableIdentifier);
        if (StringUtils.isEmpty(tableIdentifier)) {
            logger.error(ILLEGAL_ARGUMENT_MESSAGE, "table.identifier", tableIdentifier);
            throw new IllegalArgumentException("table.identifier", tableIdentifier);
        }
        String[] identifier = tableIdentifier.split("\\.");
        if (identifier.length != 2) {
            logger.error(ILLEGAL_ARGUMENT_MESSAGE, "table.identifier", tableIdentifier);
            throw new IllegalArgumentException("table.identifier", tableIdentifier);
        }
        return identifier;
    }

    /**
     * choice a Doris FE node to request.
     * @param feNodes Doris FE node list, separate be comma
     * @param logger slf4j logger
     * @return the chosen one Doris FE node
     * @throws IllegalArgumentException fe nodes is illegal
     */
    @VisibleForTesting
    static String randomEndpoint(String feNodes, Logger logger) throws IllegalArgumentException {
        logger.trace("Parse fenodes '{}'.", feNodes);
        if (StringUtils.isEmpty(feNodes)) {
            logger.error(ILLEGAL_ARGUMENT_MESSAGE, "fenodes", feNodes);
            throw new IllegalArgumentException("fenodes", feNodes);
        }
        List<String> nodes = Arrays.asList(feNodes.split(","));
        Collections.shuffle(nodes);
        return nodes.get(0).trim();
    }

    /**
     * get a valid URI to connect Doris FE.
     * @param cfg configuration of request
     * @param logger {@link Logger}
     * @return uri string
     * @throws IllegalArgumentException throw when configuration is illegal
     */
    @VisibleForTesting
    static String getUriStr(Settings cfg, Logger logger) throws IllegalArgumentException {
        String[] identifier = parseIdentifier(cfg.getProperty(DORIS_TABLE_IDENTIFIER), logger);
        return "http://" +
                randomEndpoint(cfg.getProperty(DORIS_FENODES), logger) + API_PREFIX +
                "/" + identifier[0] +
                "/" + identifier[1] +
                "/";
    }

    /**
     * discover Doris table schema from Doris FE.
     * @param cfg configuration of request
     * @param logger slf4j logger
     * @return Doris table schema
     * @throws DorisException throw when discover failed
     */
    public static Schema getSchema(Settings cfg, Logger logger)
            throws DorisException {
        logger.trace("Finding schema.");
        HttpGet httpGet = new HttpGet(getUriStr(cfg, logger) + SCHEMA);
        String response = send(cfg, httpGet, logger);
        logger.debug("Find schema response is '{}'.", response);
        return parseSchema(response, logger);
    }

    /**
     * translate Doris FE response to inner {@link Schema} struct.
     * @param response Doris FE response
     * @param logger {@link Logger}
     * @return inner {@link Schema} struct
     * @throws DorisException throw when translate failed
     */
    @VisibleForTesting
    public static Schema parseSchema(String response, Logger logger) throws DorisException {
        logger.trace("Parse response '{}' to schema.", response);
        ObjectMapper mapper = new ObjectMapper();
        Schema schema;
        try {
            schema = mapper.readValue(response, Schema.class);
        } catch (JsonParseException e) {
            String errMsg = "Doris FE's response is not a json. res: " + response;
            logger.error(errMsg, e);
            throw new DorisException(errMsg, e);
        } catch (JsonMappingException e) {
            String errMsg = "Doris FE's response cannot map to schema. res: " + response;
            logger.error(errMsg, e);
            throw new DorisException(errMsg, e);
        } catch (IOException e) {
            String errMsg = "Parse Doris FE's response to json failed. res: " + response;
            logger.error(errMsg, e);
            throw new DorisException(errMsg, e);
        }

        if (schema == null) {
            logger.error(SHOULD_NOT_HAPPEN_MESSAGE);
            throw new ShouldNeverHappenException();
        }

        if (schema.getStatus() != REST_RESPONSE_STATUS_OK) {
            String errMsg = "Doris FE's response is not OK, status is " + schema.getStatus();
            logger.error(errMsg);
            throw new DorisException(errMsg);
        }
        logger.debug("Parsing schema result is '{}'.", schema);
        return schema;
    }

    /**
     * find Doris RDD partitions from Doris FE.
     * @param cfg configuration of request
     * @param logger {@link Logger}
     * @return an list of Doris RDD partitions
     * @throws DorisException throw when find partition failed
     */
    public static List<PartitionDefinition> findPartitions(Settings cfg, Logger logger) throws DorisException {
        String[] tableIdentifiers = parseIdentifier(cfg.getProperty(DORIS_TABLE_IDENTIFIER), logger);
        String sql = "select " + cfg.getProperty(DORIS_READ_FIELD, "*") +
                " from `" + tableIdentifiers[0] + "`.`" + tableIdentifiers[1] + "`";
        if (!StringUtils.isEmpty(cfg.getProperty(DORIS_FILTER_QUERY))) {
            sql += " where " + cfg.getProperty(DORIS_FILTER_QUERY);
        }
        logger.debug("Query SQL Sending to Doris FE is: '{}'.", sql);

        HttpPost httpPost = new HttpPost(getUriStr(cfg, logger) + QUERY_PLAN);
        String entity = "{\"sql\": \""+ sql +"\"}";
        logger.debug("Post body Sending to Doris FE is: '{}'.", entity);
        StringEntity stringEntity = new StringEntity(entity, StandardCharsets.UTF_8);
        stringEntity.setContentEncoding("UTF-8");
        stringEntity.setContentType("application/json");
        httpPost.setEntity(stringEntity);

        String resStr = send(cfg, httpPost, logger);
        logger.debug("Find partition response is '{}'.", resStr);
        QueryPlan queryPlan = getQueryPlan(resStr, logger);
        Map<String, List<Long>> be2Tablets = selectBeForTablet(queryPlan, logger);
        return tabletsMapToPartition(
                cfg,
                be2Tablets,
                queryPlan.getOpaqued_query_plan(),
                tableIdentifiers[0],
                tableIdentifiers[1],
                logger);
    }

    /**
     * translate Doris FE response string to inner {@link QueryPlan} struct.
     * @param response Doris FE response string
     * @param logger {@link Logger}
     * @return inner {@link QueryPlan} struct
     * @throws DorisException throw when translate failed.
     */
    @VisibleForTesting
    static QueryPlan getQueryPlan(String response, Logger logger) throws DorisException {
        ObjectMapper mapper = new ObjectMapper();
        QueryPlan queryPlan;
        try {
            queryPlan = mapper.readValue(response, QueryPlan.class);
        } catch (JsonParseException e) {
            String errMsg = "Doris FE's response is not a json. res: " + response;
            logger.error(errMsg, e);
            throw new DorisException(errMsg, e);
        } catch (JsonMappingException e) {
            String errMsg = "Doris FE's response cannot map to schema. res: " + response;
            logger.error(errMsg, e);
            throw new DorisException(errMsg, e);
        } catch (IOException e) {
            String errMsg = "Parse Doris FE's response to json failed. res: " + response;
            logger.error(errMsg, e);
            throw new DorisException(errMsg, e);
        }

        if (queryPlan == null) {
            logger.error(SHOULD_NOT_HAPPEN_MESSAGE);
            throw new ShouldNeverHappenException();
        }

        if (queryPlan.getStatus() != REST_RESPONSE_STATUS_OK) {
            String errMsg = "Doris FE's response is not OK, status is " + queryPlan.getStatus();
            logger.error(errMsg);
            throw new DorisException(errMsg);
        }
        logger.debug("Parsing partition result is '{}'.", queryPlan);
        return queryPlan;
    }

    /**
     * select which Doris BE to get tablet data.
     * @param queryPlan {@link QueryPlan} translated from Doris FE response
     * @param logger {@link Logger}
     * @return BE to tablets {@link Map}
     * @throws DorisException throw when select failed.
     */
    @VisibleForTesting
    static  Map<String, List<Long>> selectBeForTablet(QueryPlan queryPlan, Logger logger) throws DorisException {
        Map<String, List<Long>> be2Tablets = new HashMap<>();
        for (Map.Entry<String, Tablet> part : queryPlan.getPartitions().entrySet()) {
            logger.debug("Parse tablet info: '{}'.", part);
            long tabletId;
            try {
                tabletId = Long.parseLong(part.getKey());
            } catch (NumberFormatException e) {
                String errMsg = "Parse tablet id '" + part.getKey() + "' to long failed.";
                logger.error(errMsg, e);
                throw new DorisException(errMsg, e);
            }
            String target = null;
            int tabletCount = Integer.MAX_VALUE;
            for (String candidate : part.getValue().getRoutings()) {
                logger.trace("Evaluate Doris BE '{}' to tablet '{}'.", candidate, tabletId);
                if (!be2Tablets.containsKey(candidate)) {
                    logger.debug("Choice a new Doris BE '{}' for tablet '{}'.", candidate, tabletId);
                    List<Long> tablets = new ArrayList<>();
                    be2Tablets.put(candidate, tablets);
                    target = candidate;
                    break;
                } else {
                    if (be2Tablets.get(candidate).size() < tabletCount) {
                        target = candidate;
                        tabletCount = be2Tablets.get(candidate).size();
                        logger.debug("Current candidate Doris BE to tablet '{}' is '{}' with tablet count {}.",
                                tabletId, target, tabletCount);
                    }
                }
            }
            if (target == null) {
                String errMsg = "Cannot choice Doris BE for tablet " + tabletId;
                logger.error(errMsg);
                throw new DorisException(errMsg);
            }

            logger.debug("Choice Doris BE '{}' for tablet '{}'.", target, tabletId);
            be2Tablets.get(target).add(tabletId);
        }
        return be2Tablets;
    }

    /**
     * tablet count limit for one Doris RDD partition
     * @param cfg configuration of request
     * @param logger {@link Logger}
     * @return tablet count limit
     */
    @VisibleForTesting
    static int tabletCountLimitForOnePartition(Settings cfg, Logger logger) {
        int tabletsSize = DORIS_TABLET_SIZE_DEFAULT;
        if (cfg.getProperty(DORIS_TABLET_SIZE) != null) {
            try {
                tabletsSize = Integer.parseInt(cfg.getProperty(DORIS_TABLET_SIZE));
            } catch (NumberFormatException e) {
                logger.warn(PARSE_NUMBER_FAILED_MESSAGE, DORIS_TABLET_SIZE, cfg.getProperty(DORIS_TABLET_SIZE));
            }
        }
        if (tabletsSize < DORIS_TABLET_SIZE_MIN) {
            logger.warn("{} is less than {}, set to default value {}.",
                    DORIS_TABLET_SIZE, DORIS_TABLET_SIZE_MIN, DORIS_TABLET_SIZE_MIN);
            tabletsSize = DORIS_TABLET_SIZE_MIN;
        }
        logger.debug("Tablet size is set to {}.", tabletsSize);
        return tabletsSize;
    }

    /**
     * choice a Doris BE node to request.
     * @param logger slf4j logger
     * @return the chosen one Doris BE node
     * @throws IllegalArgumentException BE nodes is illegal
     * Deprecated, use randomBackendV2 instead
     */
    @Deprecated
    @VisibleForTesting
    public static String randomBackend(SparkSettings sparkSettings , Logger logger) throws DorisException, IOException {
        String feNodes = sparkSettings.getProperty(DORIS_FENODES);
        String feNode = randomEndpoint(feNodes, logger);
        String beUrl =   String.format("http://%s" + BACKENDS, feNode);
        HttpGet httpGet = new HttpGet(beUrl);
        String response = send(sparkSettings, httpGet, logger);
        logger.info("Backend Info:{}", response);
        List<BackendRow> backends = parseBackend(response, logger);
        logger.trace("Parse beNodes '{}'.", backends);
        if (backends == null || backends.isEmpty()) {
            logger.error(ILLEGAL_ARGUMENT_MESSAGE, "beNodes", backends);
            throw new IllegalArgumentException("beNodes", String.valueOf(backends));
        }
        Collections.shuffle(backends);
        BackendRow backend = backends.get(0);
        return backend.getIP() + ":" + backend.getHttpPort();
    }

    /**
     * translate Doris FE response to inner {@link BackendRow} struct.
     * @param response Doris FE response
     * @param logger {@link Logger}
     * @return inner {@link List<BackendRow>} struct
     * @throws DorisException,IOException throw when translate failed
     * */
    @Deprecated
    @VisibleForTesting
    static List<BackendRow> parseBackend(String response, Logger logger) throws DorisException, IOException {
        com.fasterxml.jackson.databind.ObjectMapper mapper = new com.fasterxml.jackson.databind.ObjectMapper();
        Backend backend;
        try {
            backend = mapper.readValue(response, Backend.class);
        } catch (com.fasterxml.jackson.core.JsonParseException e) {
            String errMsg = "Doris BE's response is not a json. res: " + response;
            logger.error(errMsg, e);
            throw new DorisException(errMsg, e);
        } catch (com.fasterxml.jackson.databind.JsonMappingException e) {
            String errMsg = "Doris BE's response cannot map to schema. res: " + response;
            logger.error(errMsg, e);
            throw new DorisException(errMsg, e);
        } catch (IOException e) {
            String errMsg = "Parse Doris BE's response to json failed. res: " + response;
            logger.error(errMsg, e);
            throw new DorisException(errMsg, e);
        }

        if (backend == null) {
            logger.error(SHOULD_NOT_HAPPEN_MESSAGE);
            throw new ShouldNeverHappenException();
        }
        List<BackendRow> backendRows = backend.getRows().stream().filter(v -> v.getAlive()).collect(Collectors.toList());
        logger.debug("Parsing schema result is '{}'.", backendRows);
        return backendRows;
    }

    /**
     * choice a Doris BE node to request.
     * @param logger slf4j logger
     * @return the chosen one Doris BE node
     * @throws IllegalArgumentException BE nodes is illegal
     */
    @VisibleForTesting
    public static String randomBackendV2(SparkSettings sparkSettings, Logger logger) throws DorisException {
        String feNodes = sparkSettings.getProperty(DORIS_FENODES);
        String feNode = randomEndpoint(feNodes, logger);
        String beUrl =   String.format("http://%s" + BACKENDS_V2, feNode);
        HttpGet httpGet = new HttpGet(beUrl);
        String response = send(sparkSettings, httpGet, logger);
        logger.info("Backend Info:{}", response);
        List<BackendV2.BackendRowV2> backends = parseBackendV2(response, logger);
        logger.trace("Parse beNodes '{}'.", backends);
        if (backends == null || backends.isEmpty()) {
            logger.error(ILLEGAL_ARGUMENT_MESSAGE, "beNodes", backends);
            throw new IllegalArgumentException("beNodes", String.valueOf(backends));
        }
        Collections.shuffle(backends);
        BackendV2.BackendRowV2 backend = backends.get(0);
        return backend.getIp() + ":" + backend.getHttpPort();
    }

    static List<BackendV2.BackendRowV2> parseBackendV2(String response, Logger logger) throws DorisException {
        com.fasterxml.jackson.databind.ObjectMapper mapper = new com.fasterxml.jackson.databind.ObjectMapper();
        BackendV2 backend;
        try {
            backend = mapper.readValue(response, BackendV2.class);
        } catch (com.fasterxml.jackson.core.JsonParseException e) {
            String errMsg = "Doris BE's response is not a json. res: " + response;
            logger.error(errMsg, e);
            throw new DorisException(errMsg, e);
        } catch (com.fasterxml.jackson.databind.JsonMappingException e) {
            String errMsg = "Doris BE's response cannot map to schema. res: " + response;
            logger.error(errMsg, e);
            throw new DorisException(errMsg, e);
        } catch (IOException e) {
            String errMsg = "Parse Doris BE's response to json failed. res: " + response;
            logger.error(errMsg, e);
            throw new DorisException(errMsg, e);
        }

        if (backend == null) {
            logger.error(SHOULD_NOT_HAPPEN_MESSAGE);
            throw new ShouldNeverHappenException();
        }
        List<BackendV2.BackendRowV2> backendRows = backend.getBackends();
        logger.debug("Parsing schema result is '{}'.", backendRows);
        return backendRows;
    }

    /**
     * translate BE tablets map to Doris RDD partition.
     * @param cfg configuration of request
     * @param be2Tablets BE to tablets {@link Map}
     * @param opaquedQueryPlan Doris BE execute plan getting from Doris FE
     * @param database database name of Doris table
     * @param table table name of Doris table
     * @param logger {@link Logger}
     * @return Doris RDD partition {@link List}
     * @throws IllegalArgumentException throw when translate failed
     */
    @VisibleForTesting
    static List<PartitionDefinition> tabletsMapToPartition(Settings cfg, Map<String, List<Long>> be2Tablets,
            String opaquedQueryPlan, String database, String table, Logger logger)
            throws IllegalArgumentException {
        int tabletsSize = tabletCountLimitForOnePartition(cfg, logger);
        List<PartitionDefinition> partitions = new ArrayList<>();
        for (Map.Entry<String, List<Long>> beInfo : be2Tablets.entrySet()) {
            logger.debug("Generate partition with beInfo: '{}'.", beInfo);
            HashSet<Long> tabletSet = new HashSet<>(beInfo.getValue());
            beInfo.getValue().clear();
            beInfo.getValue().addAll(tabletSet);
            int first = 0;
            while (first < beInfo.getValue().size()) {
                Set<Long> partitionTablets = new HashSet<>(beInfo.getValue().subList(
                        first, Math.min(beInfo.getValue().size(), first + tabletsSize)));
                first = first + tabletsSize;
                PartitionDefinition partitionDefinition =
                        new PartitionDefinition(database, table, cfg,
                                beInfo.getKey(), partitionTablets, opaquedQueryPlan);
                logger.debug("Generate one PartitionDefinition '{}'.", partitionDefinition);
                partitions.add(partitionDefinition);
            }
        }
        return partitions;
    }
}
