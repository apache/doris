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

package org.apache.doris.tablefunction;

import org.apache.doris.common.AnalysisException;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Strings;
import com.google.gson.JsonArray;
import com.google.gson.JsonElement;
import com.google.gson.JsonObject;
import com.google.gson.JsonParser;
import org.apache.http.client.config.RequestConfig;
import org.apache.http.client.methods.HttpGet;
import org.apache.http.impl.client.CloseableHttpClient;
import org.apache.http.impl.client.HttpClientBuilder;
import org.apache.http.util.EntityUtils;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.io.IOException;
import java.nio.file.FileSystems;
import java.nio.file.PathMatcher;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Utility class for handling HuggingFace URLs and converting them to HTTP URLs.
 *
 * This class provides functionality to parse hf:// URLs and convert them to
 * actual HTTP URLs that can be used to access files on HuggingFace Hub.
 *
 * Supported URL formats:
 * - hf://datasets/username/dataset-name/path/to/file.parquet
 * - hf://datasets/username/dataset-name@revision/path/to/file.parquet
 * - hf://spaces/username/space-name/path/to/file.txt
 *
 * Example usage:
 * String hfUrl = "hf://datasets/lhoestq/demo1/default/train/0000.parquet";
 * String httpUrl = HFUtils.convertHfUrlToHttpUrl(hfUrl);
 * // Returns: https://huggingface.co/datasets/lhoestq/demo1/resolve/main/default/train/0000.parquet
 */
public class HFUtils {
    private static final Logger LOG = LogManager.getLogger(HFUtils.class);

    // Constants
    private static final String HF_SCHEME = "hf://";
    private static final String DEFAULT_ENDPOINT = "https://huggingface.co";
    private static final String DEFAULT_REVISION = "main";
    private static final String REPO_TYPE_DATASETS = "datasets";
    private static final String REPO_TYPE_SPACES = "spaces";

    // HTTP Client Configuration
    private static final int DEFAULT_TIMEOUT_MS = 30000; // 30 seconds
    private static final int DEFAULT_CONNECT_TIMEOUT_MS = 10000; // 10 seconds
    private static final int DEFAULT_PAGE_LIMIT = 1000;

    /**
     * Parsed HuggingFace URL components
     */
    public static class ParsedHFUrl {
        private String endpoint = DEFAULT_ENDPOINT;
        private String repoType;
        private String repository;
        private String revision = DEFAULT_REVISION;
        private String path;

        public String getEndpoint() {
            return endpoint;
        }

        public void setEndpoint(String endpoint) {
            this.endpoint = endpoint;
        }

        public String getRepoType() {
            return repoType;
        }

        public void setRepoType(String repoType) {
            this.repoType = repoType;
        }

        public String getRepository() {
            return repository;
        }

        public void setRepository(String repository) {
            this.repository = repository;
        }

        public String getRevision() {
            return revision;
        }

        public void setRevision(String revision) {
            this.revision = revision;
        }

        public String getPath() {
            return path;
        }

        public void setPath(String path) {
            this.path = path;
        }

        @Override
        public String toString() {
            return String.format("ParsedHFUrl{endpoint='%s', repoType='%s', repository='%s', revision='%s', path='%s'}",
                    endpoint, repoType, repository, revision, path);
        }
    }

    /**
     * Convert a HuggingFace URL to an HTTP URL
     *
     * @param hfUrl The hf:// URL to convert
     * @return The corresponding HTTP URL
     * @throws AnalysisException if the URL format is invalid
     */
    @VisibleForTesting
    public static String convertHfUrlToHttpUrl(String hfUrl) throws AnalysisException {
        if (Strings.isNullOrEmpty(hfUrl)) {
            throw new AnalysisException("HuggingFace URL cannot be null or empty");
        }

        ParsedHFUrl parsedUrl = parseHfUrl(hfUrl);
        return buildHttpUrl(parsedUrl);
    }

    /**
     * Parse a HuggingFace URL into its components
     *
     * @param url The hf:// URL to parse
     * @return ParsedHFUrl object containing the parsed components
     * @throws AnalysisException if the URL format is invalid
     */
    @VisibleForTesting
    public static ParsedHFUrl parseHfUrl(String url) throws AnalysisException {
        if (Strings.isNullOrEmpty(url)) {
            throw new AnalysisException("URL cannot be null or empty");
        }

        if (!url.startsWith(HF_SCHEME)) {
            throw new AnalysisException("URL must start with 'hf://', got: " + url);
        }

        ParsedHFUrl result = new ParsedHFUrl();

        // Remove the hf:// prefix
        String remaining = url.substring(HF_SCHEME.length());

        if (remaining.isEmpty()) {
            throwParseError(url);
        }

        String[] parts = remaining.split("/", -1); // -1 to keep empty strings

        if (parts.length < 4) {
            throwParseError(url);
        }

        // Parse repository type
        result.setRepoType(parts[0]);
        if (!REPO_TYPE_DATASETS.equals(result.getRepoType()) && !REPO_TYPE_SPACES.equals(result.getRepoType())) {
            throw new AnalysisException(
                String.format("Currently only supports 'datasets' and 'spaces' repository types, got: '%s' in URL: %s",
                    result.getRepoType(), url));
        }

        // Parse username and repository name
        String username = parts[1];
        String repoName = parts[2];

        if (username.isEmpty() || repoName.isEmpty()) {
            throwParseError(url);
        }

        // Check if repository name contains revision
        int atIndex = repoName.indexOf('@');
        if (atIndex != -1) {
            String actualRepoName = repoName.substring(0, atIndex);
            result.setRevision(repoName.substring(atIndex + 1));

            if (actualRepoName.isEmpty() || result.getRevision().isEmpty()) {
                throwParseError(url);
            }
            result.setRepository(username + "/" + actualRepoName);
        } else {
            result.setRepository(username + "/" + repoName);
        }

        // Build the path from remaining parts
        StringBuilder pathBuilder = new StringBuilder();
        for (int i = 3; i < parts.length; i++) {
            pathBuilder.append("/").append(parts[i]);
        }
        String rawPath = pathBuilder.toString();

        // Handle HuggingFace web interface paths like /blob/main/ or /tree/main/
        // These should be converted to proper API paths
        if (rawPath.startsWith("/blob/") || rawPath.startsWith("/tree/")) {
            // Extract revision and actual path
            String[] pathParts = rawPath.substring(1).split("/", 3); // Remove leading slash and split
            if (pathParts.length >= 2) {
                // pathParts[0] is "blob" or "tree" - we don't need to use it
                String pathRevision = pathParts[1]; // revision like "main"
                String actualPath = pathParts.length > 2 ? "/" + pathParts[2] : "";

                // Use the revision from the path if not already specified via @
                if (result.getRevision().equals(DEFAULT_REVISION)) {
                    result.setRevision(pathRevision);
                }

                result.setPath(actualPath);
            } else {
                result.setPath(rawPath);
            }
        } else {
            result.setPath(rawPath);
        }

        // If no path parts exist, set to empty string
        if (result.getPath().isEmpty()) {
            result.setPath("");
        }
        // Note: if path is "/" (from trailing slash), keep it as is

        LOG.debug("Parsed HF URL: {} -> {}", url, result);
        return result;
    }

    /**
     * Build HTTP URL from parsed HF URL components
     *
     * @param parsedUrl The parsed HF URL components
     * @return The HTTP URL string
     */
    private static String buildHttpUrl(ParsedHFUrl parsedUrl) {
        // URL format: {endpoint}/{repo_type}/{repository}/resolve/{revision}{path}
        StringBuilder httpUrl = new StringBuilder();

        httpUrl.append(parsedUrl.getEndpoint());
        if (!parsedUrl.getEndpoint().endsWith("/")) {
            httpUrl.append("/");
        }

        httpUrl.append(parsedUrl.getRepoType()).append("/");
        httpUrl.append(parsedUrl.getRepository()).append("/");
        httpUrl.append("resolve").append("/");
        httpUrl.append(parsedUrl.getRevision());
        httpUrl.append(parsedUrl.getPath());

        String result = httpUrl.toString();
        LOG.debug("Built HTTP URL: {}", result);
        return result;
    }

    /**
     * Validate if a URL is a valid HuggingFace URL
     *
     * @param url The URL to validate
     * @return true if it's a valid hf:// URL, false otherwise
     */
    @VisibleForTesting
    public static boolean isValidHfUrl(String url) {
        if (Strings.isNullOrEmpty(url)) {
            return false;
        }

        try {
            parseHfUrl(url);
            return true;
        } catch (AnalysisException e) {
            LOG.debug("Invalid HF URL: {}, error: {}", url, e.getMessage());
            return false;
        }
    }

    /**
     * Get the tree API URL for listing files in a repository
     * This is useful for implementing glob patterns or directory listing
     *
     * @param parsedUrl The parsed HF URL components
     * @param limit Optional limit for the number of results (0 means no limit)
     * @return The tree API URL
     */
    @VisibleForTesting
    public static String buildTreeApiUrl(ParsedHFUrl parsedUrl, int limit) {
        return buildTreeApiUrl(parsedUrl, parsedUrl.getPath(), limit);
    }

    /**
     * Get the tree API URL for listing files in a specific path
     *
     * @param parsedUrl The parsed HF URL components
     * @param path The specific path to list
     * @param limit Optional limit for the number of results (0 means no limit)
     * @return The tree API URL
     */
    private static String buildTreeApiUrl(ParsedHFUrl parsedUrl, String path, int limit) {
        // URL format: {endpoint}/api/{repo_type}/{repository}/tree/{revision}{path}
        StringBuilder treeUrl = new StringBuilder();

        treeUrl.append(parsedUrl.getEndpoint());
        if (!parsedUrl.getEndpoint().endsWith("/")) {
            treeUrl.append("/");
        }

        treeUrl.append("api").append("/");
        treeUrl.append(parsedUrl.getRepoType()).append("/");
        treeUrl.append(parsedUrl.getRepository()).append("/");
        treeUrl.append("tree").append("/");
        treeUrl.append(parsedUrl.getRevision());

        // Add path if provided
        if (!Strings.isNullOrEmpty(path) && !"/".equals(path)) {
            if (!path.startsWith("/")) {
                treeUrl.append("/");
            }
            treeUrl.append(path);
        }

        if (limit > 0) {
            treeUrl.append("?limit=").append(limit);
        }

        String result = treeUrl.toString();
        LOG.debug("Built tree API URL: {}", result);
        return result;
    }

    /**
     * Extract repository information from HF URL for display purposes
     *
     * @param hfUrl The hf:// URL
     * @return A human-readable repository description
     * @throws AnalysisException if the URL is invalid
     */
    @VisibleForTesting
    public static String getRepositoryInfo(String hfUrl) throws AnalysisException {
        ParsedHFUrl parsed = parseHfUrl(hfUrl);
        return String.format("%s/%s@%s", parsed.getRepoType(), parsed.getRepository(), parsed.getRevision());
    }

    /**
     * Expand a HuggingFace URL with glob patterns to matching file URLs
     *
     * @param hfGlobUrl The hf:// URL with glob patterns
     * @return List of HTTP URLs that match the glob pattern
     * @throws AnalysisException if the URL format is invalid or glob processing fails
     */
    public static List<String> expandGlob(String hfGlobUrl) throws AnalysisException {
        return expandGlob(hfGlobUrl, null);
    }

    /**
     * Expand a HuggingFace URL with glob patterns to matching file URLs
     *
     * @param hfGlobUrl The hf:// URL with glob patterns
     * @param authToken Optional authentication token for private repositories
     * @return List of HTTP URLs that match the glob pattern
     * @throws AnalysisException if the URL format is invalid or glob processing fails
     */
    public static List<String> expandGlob(String hfGlobUrl, String authToken) throws AnalysisException {
        if (Strings.isNullOrEmpty(hfGlobUrl)) {
            throw new AnalysisException("HuggingFace glob URL cannot be null or empty");
        }

        // Parse the glob URL
        ParsedHFUrl parsedUrl = parseHfUrl(hfGlobUrl);

        // Check if the path contains wildcard characters
        String path = parsedUrl.getPath();
        if (!containsWildcards(path)) {
            // No wildcards, return the single file
            List<String> result = new ArrayList<>();
            result.add(buildHttpUrl(parsedUrl));
            return result;
        }

        // Find the longest prefix without wildcards
        String sharedPath = getLongestPrefixWithoutWildcards(path);

        // Prepare headers
        Map<String, String> headers = new HashMap<>();
        if (!Strings.isNullOrEmpty(authToken)) {
            headers.put("Authorization", "Bearer " + authToken);
        }

        List<String> result = new ArrayList<>();

        try {
            // Get all files and directories to process
            List<String> pathsToProcess = new ArrayList<>();
            pathsToProcess.add(sharedPath);

            List<String> allFilePaths = new ArrayList<>();

            // Calculate the depth needed for recursion
            // Count the number of path components in the pattern after the shared prefix
            String remainingPattern = path.substring(sharedPath.length());
            int patternDepth = splitPath(remainingPattern).size();

            // If pattern contains **, we need unlimited recursion
            boolean unlimitedRecursion = path.contains("**");

            // For a pattern like /*/*.parquet (depth=2), we need to recurse into depth 1
            // to list files at depth 2. So maxRecursionDepth = patternDepth - 1
            // But if patternDepth is 1 or less, we still need depth 0, so use Math.max
            int maxRecursionDepth = unlimitedRecursion ? Integer.MAX_VALUE : Math.max(0, patternDepth - 1);

            // Track depth for each path being processed
            Map<String, Integer> pathDepths = new HashMap<>();
            pathDepths.put(sharedPath, 0);

            // Process directories recursively if needed
            while (!pathsToProcess.isEmpty()) {
                String currentPath = pathsToProcess.remove(0);
                int currentDepth = pathDepths.getOrDefault(currentPath, 0);

                // List files in current directory
                List<String> files = new ArrayList<>();
                List<String> directories = new ArrayList<>();
                listHuggingFaceFiles(parsedUrl, currentPath, headers, files, directories);

                // Add all file paths
                allFilePaths.addAll(files);

                // Add directories for recursive processing based on pattern depth
                // We need to recurse if current depth is less than max recursion depth
                if (currentDepth < maxRecursionDepth) {
                    for (String dir : directories) {
                        pathsToProcess.add(dir);
                        pathDepths.put(dir, currentDepth + 1);
                    }
                }
            }

            // Filter files using glob pattern matching
            List<String> patternComponents = splitPath(path);

            for (String filePath : allFilePaths) {
                List<String> fileComponents = splitPath(filePath);

                if (matchPathComponents(fileComponents, patternComponents)) {
                    // Build the complete HTTP URL for the matched file
                    ParsedHFUrl fileUrl = new ParsedHFUrl();
                    fileUrl.setEndpoint(parsedUrl.getEndpoint());
                    fileUrl.setRepoType(parsedUrl.getRepoType());
                    fileUrl.setRepository(parsedUrl.getRepository());
                    fileUrl.setRevision(parsedUrl.getRevision());
                    fileUrl.setPath(filePath);

                    String httpUrl = buildHttpUrl(fileUrl);
                    result.add(httpUrl);
                }
            }

        } catch (Exception e) {
            throw new AnalysisException("Failed to expand glob pattern: " + e.getMessage());
        }

        return result;
    }

    /**
     * Create HTTP client with proper configuration
     */
    private static CloseableHttpClient createHttpClient() {
        RequestConfig config = RequestConfig.custom()
                .setConnectTimeout(DEFAULT_CONNECT_TIMEOUT_MS)
                .setConnectionRequestTimeout(DEFAULT_TIMEOUT_MS)
                .setSocketTimeout(DEFAULT_TIMEOUT_MS)
                .build();

        return HttpClientBuilder.create()
                .setDefaultRequestConfig(config)
                .build();
    }

    /**
     * Execute HTTP GET request
     */
    private static String executeHttpGet(String url, Map<String, String> headers) throws IOException {
        try (CloseableHttpClient client = createHttpClient()) {
            HttpGet httpGet = new HttpGet(url);

            // Set headers
            if (headers != null) {
                for (Map.Entry<String, String> entry : headers.entrySet()) {
                    httpGet.setHeader(entry.getKey(), entry.getValue());
                }
            }

            // Set User-Agent
            httpGet.setHeader("User-Agent", "Doris-HFUtils/1.0");

            return client.execute(httpGet, response -> {
                int statusCode = response.getStatusLine().getStatusCode();
                String responseBody = EntityUtils.toString(response.getEntity());

                if (statusCode >= 400) {
                    throw new IOException("HTTP " + statusCode + ": " + responseBody);
                }

                return responseBody;
            });
        }
    }

    /**
     * List files from HuggingFace API with pagination support
     */
    private static void listHuggingFaceFiles(ParsedHFUrl parsedUrl, String path,
                                           Map<String, String> headers,
                                           List<String> files, List<String> directories) throws AnalysisException {
        // Build API URL
        String apiUrl = buildTreeApiUrl(parsedUrl, path, DEFAULT_PAGE_LIMIT);

        String nextUrl = apiUrl;
        int pageCount = 0;

        while (nextUrl != null && pageCount < 100) { // Prevent infinite loops
            try {
                String response = executeHttpGet(nextUrl, headers);

                // Parse JSON response
                JsonArray jsonArray = JsonParser.parseString(response).getAsJsonArray();

                for (JsonElement element : jsonArray) {
                    JsonObject obj = element.getAsJsonObject();

                    String filePath = "/" + obj.get("path").getAsString();
                    String type = obj.get("type").getAsString();

                    if ("file".equals(type)) {
                        files.add(filePath);
                    } else if ("directory".equals(type)) {
                        directories.add(filePath);
                    }
                }

                // For simplicity, we don't handle pagination in this basic version
                // In a real implementation, you would parse Link headers here
                nextUrl = null;
                pageCount++;

            } catch (Exception e) {
                throw new AnalysisException("Failed to list files from HuggingFace API: " + e.getMessage());
            }
        }
    }

    /**
     * Check if a path contains wildcard characters
     *
     * @param path The path to check
     * @return true if the path contains wildcards, false otherwise
     */
    @VisibleForTesting
    public static boolean containsWildcards(String path) {
        if (Strings.isNullOrEmpty(path)) {
            return false;
        }
        return path.contains("*") || path.contains("?") || path.contains("[") || path.contains("{");
    }

    /**
     * Get the longest prefix of a path that doesn't contain wildcards
     *
     * @param path The path to analyze
     * @return The longest prefix without wildcards
     */
    @VisibleForTesting
    public static String getLongestPrefixWithoutWildcards(String path) {
        if (Strings.isNullOrEmpty(path)) {
            return "";
        }

        int firstWildcardPos = -1;
        for (int i = 0; i < path.length(); i++) {
            char c = path.charAt(i);
            if (c == '*' || c == '?' || c == '[' || c == '{') {
                firstWildcardPos = i;
                break;
            }
        }

        if (firstWildcardPos == -1) {
            return path; // No wildcards found
        }

        // Find the last slash before the first wildcard
        String prefix = path.substring(0, firstWildcardPos);
        int lastSlash = prefix.lastIndexOf('/');

        if (lastSlash == -1) {
            return ""; // Root path
        }

        return path.substring(0, lastSlash);
    }

    /**
     * Match a file path against a glob pattern
     * This is a simplified implementation based on DuckDB's Match function
     *
     * @param filePath The file path to match
     * @param globPattern The glob pattern
     * @return true if the file matches the pattern, false otherwise
     */
    @VisibleForTesting
    public static boolean matchGlobPattern(String filePath, String globPattern) {
        if (Strings.isNullOrEmpty(filePath) || Strings.isNullOrEmpty(globPattern)) {
            return false;
        }

        try {
            // Use Java's built-in glob pattern matching
            PathMatcher matcher = FileSystems.getDefault().getPathMatcher("glob:" + globPattern);
            return matcher.matches(Paths.get(filePath));
        } catch (Exception e) {
            LOG.warn("Failed to match glob pattern: {} against file: {}, error: {}",
                    globPattern, filePath, e.getMessage());
            return false;
        }
    }

    /**
     * Split a path into components for pattern matching
     *
     * @param path The path to split
     * @return List of path components
     */
    @VisibleForTesting
    public static List<String> splitPath(String path) {
        if (Strings.isNullOrEmpty(path)) {
            return new ArrayList<>();
        }

        List<String> components = new ArrayList<>();
        String[] parts = path.split("/");
        for (String part : parts) {
            if (!part.isEmpty()) {
                components.add(part);
            }
        }
        return components;
    }

    /**
     * Advanced pattern matching similar to DuckDB's Match function
     * Supports ** for recursive matching
     *
     * @param pathComponents The path components to match
     * @param patternComponents The pattern components
     * @return true if the path matches the pattern, false otherwise
     */
    @VisibleForTesting
    public static boolean matchPathComponents(List<String> pathComponents, List<String> patternComponents) {
        return matchPathComponentsRecursive(pathComponents, 0, patternComponents, 0);
    }

    private static boolean matchPathComponentsRecursive(List<String> pathComponents, int pathIndex,
                                                       List<String> patternComponents, int patternIndex) {
        // Base cases
        if (pathIndex >= pathComponents.size() && patternIndex >= patternComponents.size()) {
            return true; // Both exhausted, match
        }
        if (patternIndex >= patternComponents.size()) {
            return false; // Pattern exhausted but path remains
        }
        if (pathIndex >= pathComponents.size()) {
            // Path exhausted, check if remaining pattern is all **
            for (int i = patternIndex; i < patternComponents.size(); i++) {
                if (!"**".equals(patternComponents.get(i))) {
                    return false;
                }
            }
            return true;
        }

        String currentPattern = patternComponents.get(patternIndex);

        if ("**".equals(currentPattern)) {
            // ** matches zero or more path components
            if (patternIndex + 1 >= patternComponents.size()) {
                return true; // ** at end matches everything
            }

            // Try matching ** with 0, 1, 2, ... path components
            for (int i = pathIndex; i <= pathComponents.size(); i++) {
                if (matchPathComponentsRecursive(pathComponents, i, patternComponents, patternIndex + 1)) {
                    return true;
                }
            }
            return false;
        } else {
            // Regular pattern matching (including * and [])
            String currentPath = pathComponents.get(pathIndex);
            if (matchGlobPattern(currentPath, currentPattern)) {
                return matchPathComponentsRecursive(pathComponents, pathIndex + 1, patternComponents, patternIndex + 1);
            }
            return false;
        }
    }

    /**
     * Validate if a URL contains valid glob patterns
     *
     * @param hfUrl The hf:// URL to validate
     * @return true if it's a valid glob URL, false otherwise
     */
    @VisibleForTesting
    public static boolean isValidGlobUrl(String hfUrl) {
        if (!isValidHfUrl(hfUrl)) {
            return false;
        }

        try {
            ParsedHFUrl parsed = parseHfUrl(hfUrl);
            return containsWildcards(parsed.getPath());
        } catch (AnalysisException e) {
            return false;
        }
    }

    private static void throwParseError(String url) throws AnalysisException {
        throw new AnalysisException(
            String.format("Failed to parse HuggingFace URL: '%s'. "
                + "Please format URL like: 'hf://datasets/username/dataset-name/path/to/file.parquet' "
                + "or 'hf://datasets/username/dataset-name@revision/path/to/file.parquet'", url));
    }
}

