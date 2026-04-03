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

package org.apache.doris.httpv2.rest.manager;

import org.apache.doris.common.Config;
import org.apache.doris.common.util.NetUtils;
import org.apache.doris.system.SystemInfoService.HostInfo;

import com.google.common.base.Strings;
import lombok.Data;

import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.time.LocalDateTime;
import java.time.OffsetDateTime;
import java.time.ZoneId;
import java.time.format.DateTimeFormatter;
import java.time.format.DateTimeParseException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.Comparator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.stream.Collectors;
import java.util.stream.Stream;

public class LogQueryService {
    public static final String REDUCTION_GROUPED = "grouped";
    public static final String REDUCTION_RAW = "raw";

    private static final int DEFAULT_MAX_ENTRIES = 20;
    private static final int MAX_MAX_ENTRIES = 200;
    private static final int DEFAULT_MAX_BYTES_PER_NODE = 256 * 1024;
    private static final int MAX_MAX_BYTES_PER_NODE = 1024 * 1024;
    private static final int MAX_EXAMPLES_PER_GROUP = 2;
    private static final int MAX_EVENT_TEXT_LENGTH = 4096;

    private static final DateTimeFormatter FE_TIME_FORMATTER =
            DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss,SSS");
    private static final DateTimeFormatter FE_TIME_SECOND_FORMATTER =
            DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss");
    private static final DateTimeFormatter BE_TIME_FORMATTER =
            DateTimeFormatter.ofPattern("yyyyMMdd HH:mm:ss.SSSSSS");
    private static final DateTimeFormatter GC_TIME_FORMATTER =
            DateTimeFormatter.ofPattern("yyyy-MM-dd'T'HH:mm:ss.SSSZ");

    private static final Pattern FE_EVENT_PATTERN = Pattern.compile(
            "^\\d{4}-\\d{2}-\\d{2} \\d{2}:\\d{2}:\\d{2}(,\\d{3})?.*");
    private static final Pattern BE_EVENT_PATTERN = Pattern.compile(
            "^[IWEF]\\d{8} \\d{2}:\\d{2}:\\d{2}\\.\\d{6}.*");
    private static final Pattern GC_EVENT_PATTERN = Pattern.compile(
            "^\\[(\\d{4}-\\d{2}-\\d{2}T\\d{2}:\\d{2}:\\d{2}\\.\\d{3}[+-]\\d{4})\\].*");
    private static final Pattern UUID_PATTERN = Pattern.compile(
            "(?i)\\b[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}\\b");
    private static final Pattern HOST_PORT_PATTERN = Pattern.compile(
            "\\b(?:\\d{1,3}\\.){3}\\d{1,3}:\\d+\\b");
    private static final Pattern IP_PATTERN = Pattern.compile(
            "\\b(?:\\d{1,3}\\.){3}\\d{1,3}\\b");
    private static final Pattern HEX_PATTERN = Pattern.compile("(?i)\\b0x[0-9a-f]+\\b");
    private static final Pattern LARGE_NUMBER_PATTERN = Pattern.compile("\\b\\d{4,}\\b");

    public QueryRequest normalize(QueryRequest request) {
        QueryRequest normalized = request == null ? new QueryRequest() : request.copy();
        if (normalized.getLogTypes() == null || normalized.getLogTypes().isEmpty()) {
            throw new IllegalArgumentException("logTypes is required");
        }
        normalized.setLogTypes(normalized.getLogTypes().stream()
                .filter(Objects::nonNull)
                .map(type -> type.trim().toLowerCase(Locale.ROOT))
                .filter(type -> !type.isEmpty())
                .distinct()
                .collect(Collectors.toList()));
        if (normalized.getLogTypes().isEmpty()) {
            throw new IllegalArgumentException("logTypes is required");
        }
        for (String logType : normalized.getLogTypes()) {
            if (!LogType.fromName(logType).isPresent()) {
                throw new IllegalArgumentException("Unsupported log type: " + logType);
            }
        }
        if (normalized.getStartTimeMs() != null && normalized.getEndTimeMs() != null
                && normalized.getStartTimeMs() > normalized.getEndTimeMs()) {
            throw new IllegalArgumentException("startTimeMs must be <= endTimeMs");
        }

        normalized.setReductionMode(normalizeReductionMode(normalized.getReductionMode()));
        normalized.setMaxEntries(clamp(normalized.getMaxEntries(), DEFAULT_MAX_ENTRIES, 1, MAX_MAX_ENTRIES));
        normalized.setMaxBytesPerNode(clamp(normalized.getMaxBytesPerNode(), DEFAULT_MAX_BYTES_PER_NODE,
                8 * 1024, MAX_MAX_BYTES_PER_NODE));
        normalized.setKeyword(Strings.emptyToNull(normalized.getKeyword()));
        normalized.setFrontendNodes(normalizeNodes(normalized.getFrontendNodes()));
        normalized.setBackendNodes(normalizeNodes(normalized.getBackendNodes()));
        return normalized;
    }

    public QueryRequest filterForFrontend(QueryRequest request) {
        QueryRequest filtered = request.copy();
        filtered.setLogTypes(request.getLogTypes().stream()
                .filter(type -> LogType.fromName(type).map(LogType::isFrontend).orElse(false))
                .collect(Collectors.toList()));
        return filtered;
    }

    public QueryRequest filterForBackend(QueryRequest request) {
        QueryRequest filtered = request.copy();
        filtered.setLogTypes(request.getLogTypes().stream()
                .filter(type -> LogType.fromName(type).map(LogType::isBackend).orElse(false))
                .collect(Collectors.toList()));
        return filtered;
    }

    public NodeQueryPayload queryFrontendNode(QueryRequest request, String nodeName) {
        QueryRequest normalized = normalize(filterForFrontend(request));
        List<LogType> logTypes = normalized.getLogTypes().stream()
                .map(type -> LogType.fromName(type)
                        .orElseThrow(() -> new IllegalArgumentException("Unsupported log type: " + type)))
                .collect(Collectors.toList());
        List<NodeQueryResult> results = new ArrayList<>();
        if (logTypes.isEmpty()) {
            return new NodeQueryPayload(results);
        }

        String logDir = getFrontendLogDir();
        int bytesPerType = Math.max(8 * 1024, normalized.getMaxBytesPerNode() / logTypes.size());
        for (LogType logType : logTypes) {
            results.add(querySingleType(normalized, nodeName, "FE", logDir, logType, bytesPerType));
        }
        return new NodeQueryPayload(results);
    }

    private NodeQueryResult querySingleType(QueryRequest request, String nodeName, String nodeType, String logDir,
            LogType logType, int maxBytes) {
        NodeQueryResult result = new NodeQueryResult();
        result.setNode(nodeName);
        result.setNodeType(nodeType);
        result.setLogType(logType.typeName);
        result.setReductionMode(request.getReductionMode());
        result.setScannedFiles(new ArrayList<>());

        List<Path> candidateFiles = listCandidateFiles(logDir, logType);
        if (candidateFiles.isEmpty()) {
            result.setError("No log files matched the log type");
            result.setGroups(Collections.emptyList());
            result.setEvents(Collections.emptyList());
            return result;
        }

        String content;
        try {
            content = readRecentText(candidateFiles, result.getScannedFiles(), maxBytes);
        } catch (IOException e) {
            result.setError("Failed to read log files: " + e.getMessage());
            result.setGroups(Collections.emptyList());
            result.setEvents(Collections.emptyList());
            return result;
        }

        List<ParsedEvent> events = parseEvents(content, logType);
        List<ParsedEvent> filteredEvents = applyFilters(events, request);
        result.setMatchedEventCount(filteredEvents.size());
        if (REDUCTION_RAW.equals(request.getReductionMode())) {
            List<ParsedEvent> ordered = filteredEvents.stream()
                    .sorted(Comparator.comparingLong(ParsedEvent::getSortTimeMs).reversed())
                    .collect(Collectors.toList());
            result.setTruncated(ordered.size() > request.getMaxEntries());
            List<LogEventView> eventViews = ordered.stream()
                    .limit(request.getMaxEntries())
                    .map(this::toLogEventView)
                    .collect(Collectors.toList());
            result.setReturnedItemCount(eventViews.size());
            result.setEvents(eventViews);
            result.setGroups(Collections.emptyList());
            return result;
        }

        List<LogGroupView> groups = buildGroups(filteredEvents, request.getMaxEntries());
        result.setReturnedItemCount(groups.size());
        result.setTruncated(groups.size() < countDistinctPatterns(filteredEvents));
        result.setGroups(groups);
        result.setEvents(Collections.emptyList());
        return result;
    }

    private List<LogGroupView> buildGroups(List<ParsedEvent> events, int maxEntries) {
        Map<String, MutableGroup> grouped = new LinkedHashMap<>();
        for (ParsedEvent event : events) {
            String key = normalizePattern(event.getFirstLine());
            MutableGroup group = grouped.computeIfAbsent(key, unused -> new MutableGroup(key));
            group.add(event);
        }
        return grouped.values().stream()
                .sorted(Comparator.comparingInt(MutableGroup::getCount).reversed()
                        .thenComparingLong(MutableGroup::getLastTimeMs).reversed())
                .limit(maxEntries)
                .map(MutableGroup::toView)
                .collect(Collectors.toList());
    }

    private int countDistinctPatterns(List<ParsedEvent> events) {
        return (int) events.stream().map(event -> normalizePattern(event.getFirstLine())).distinct().count();
    }

    private LogEventView toLogEventView(ParsedEvent event) {
        LogEventView view = new LogEventView();
        view.setTimeMs(event.getTimeMs());
        view.setFirstLine(event.getFirstLine());
        view.setMessage(trimText(event.getMessage()));
        return view;
    }

    private List<ParsedEvent> applyFilters(List<ParsedEvent> events, QueryRequest request) {
        String keyword = request.getKeyword() == null ? null : request.getKeyword().toLowerCase(Locale.ROOT);
        List<ParsedEvent> filtered = new ArrayList<>();
        for (ParsedEvent event : events) {
            if (request.getStartTimeMs() != null && event.getTimeMs() != null
                    && event.getTimeMs() < request.getStartTimeMs()) {
                continue;
            }
            if (request.getEndTimeMs() != null && event.getTimeMs() != null
                    && event.getTimeMs() >= request.getEndTimeMs()) {
                continue;
            }
            if (keyword != null && !event.getMessage().toLowerCase(Locale.ROOT).contains(keyword)) {
                continue;
            }
            filtered.add(event);
        }
        return filtered;
    }

    List<ParsedEvent> parseEvents(String content, LogType logType) {
        if (Strings.isNullOrEmpty(content)) {
            return Collections.emptyList();
        }
        List<ParsedEvent> events = new ArrayList<>();
        ParsedEvent current = null;
        for (String rawLine : content.split("\\r?\\n")) {
            String line = rawLine == null ? "" : rawLine;
            if (isEventStart(line, logType)) {
                if (current != null) {
                    events.add(current.finish());
                }
                current = new ParsedEvent();
                current.append(line);
                current.setFirstLine(line);
                current.setTimeMs(parseTimeMs(line, logType).orElse(null));
            } else if (current != null) {
                current.append(line);
            }
        }
        if (current != null) {
            events.add(current.finish());
        }
        return events;
    }

    private boolean isEventStart(String line, LogType logType) {
        switch (logType) {
            case FE_INFO:
            case FE_WARN:
            case BE_JNI:
                return FE_EVENT_PATTERN.matcher(line).matches();
            case BE_INFO:
            case BE_WARNING:
                return BE_EVENT_PATTERN.matcher(line).matches();
            case FE_GC:
            case BE_GC:
                return GC_EVENT_PATTERN.matcher(line).matches();
            default:
                return false;
        }
    }

    private Optional<Long> parseTimeMs(String line, LogType logType) {
        try {
            switch (logType) {
                case FE_INFO:
                case FE_WARN:
                    return Optional.of(LocalDateTime.parse(line.substring(0, 23), FE_TIME_FORMATTER)
                            .atZone(ZoneId.systemDefault()).toInstant().toEpochMilli());
                case BE_JNI:
                    String prefix = line.length() >= 23 ? line.substring(0, 23) : line;
                    if (prefix.length() >= 23 && prefix.charAt(19) == ',') {
                        return Optional.of(LocalDateTime.parse(prefix, FE_TIME_FORMATTER)
                                .atZone(ZoneId.systemDefault()).toInstant().toEpochMilli());
                    }
                    return Optional.of(LocalDateTime.parse(line.substring(0, 19), FE_TIME_SECOND_FORMATTER)
                            .atZone(ZoneId.systemDefault()).toInstant().toEpochMilli());
                case BE_INFO:
                case BE_WARNING:
                    return Optional.of(LocalDateTime.parse(line.substring(1, 24), BE_TIME_FORMATTER)
                            .atZone(ZoneId.systemDefault()).toInstant().toEpochMilli());
                case FE_GC:
                case BE_GC:
                    Matcher matcher = GC_EVENT_PATTERN.matcher(line);
                    if (matcher.matches()) {
                        return Optional.of(OffsetDateTime.parse(matcher.group(1), GC_TIME_FORMATTER)
                                .toInstant().toEpochMilli());
                    }
                    return Optional.empty();
                default:
                    return Optional.empty();
            }
        } catch (DateTimeParseException | IndexOutOfBoundsException e) {
            return Optional.empty();
        }
    }

    private String normalizePattern(String line) {
        if (Strings.isNullOrEmpty(line)) {
            return "<EMPTY>";
        }
        String normalized = line;
        normalized = UUID_PATTERN.matcher(normalized).replaceAll("<UUID>");
        normalized = HOST_PORT_PATTERN.matcher(normalized).replaceAll("<HOST:PORT>");
        normalized = IP_PATTERN.matcher(normalized).replaceAll("<IP>");
        normalized = HEX_PATTERN.matcher(normalized).replaceAll("<HEX>");
        normalized = LARGE_NUMBER_PATTERN.matcher(normalized).replaceAll("<NUM>");
        return normalized;
    }

    private List<Path> listCandidateFiles(String logDir, LogType logType) {
        Path dir = Paths.get(logDir);
        if (!Files.isDirectory(dir)) {
            return Collections.emptyList();
        }
        try (Stream<Path> pathStream = Files.list(dir)) {
            return pathStream
                    .filter(Files::isRegularFile)
                    .filter(path -> logType.matches(path.getFileName().toString()))
                    .sorted(Comparator.comparingLong(this::lastModified).reversed())
                    .collect(Collectors.toList());
        } catch (IOException e) {
            return Collections.emptyList();
        }
    }

    private String readRecentText(List<Path> files, List<String> scannedFiles, int maxBytes) throws IOException {
        List<String> chunks = new ArrayList<>();
        int remaining = maxBytes;
        for (Path file : files) {
            if (remaining <= 0) {
                break;
            }
            long size = Files.size(file);
            scannedFiles.add(file.getFileName().toString());
            if (size <= remaining) {
                chunks.add(new String(Files.readAllBytes(file), StandardCharsets.UTF_8));
                remaining -= (int) size;
            } else {
                chunks.add(readTail(file, remaining));
                remaining = 0;
            }
        }
        Collections.reverse(chunks);
        return String.join("\n", chunks);
    }

    private String readTail(Path path, int bytes) throws IOException {
        try (RandomAccessFile raf = new RandomAccessFile(path.toFile(), "r")) {
            long startPos = Math.max(0L, raf.length() - bytes);
            raf.seek(startPos);
            byte[] data = new byte[(int) (raf.length() - startPos)];
            raf.readFully(data);
            return new String(data, StandardCharsets.UTF_8);
        }
    }

    private long lastModified(Path path) {
        try {
            return Files.getLastModifiedTime(path).toMillis();
        } catch (IOException e) {
            return 0L;
        }
    }

    private String getFrontendLogDir() {
        if (!Strings.isNullOrEmpty(Config.sys_log_dir)) {
            return Config.sys_log_dir;
        }
        String logDir = System.getenv("LOG_DIR");
        if (!Strings.isNullOrEmpty(logDir)) {
            return logDir;
        }
        String dorisHome = System.getenv("DORIS_HOME");
        if (!Strings.isNullOrEmpty(dorisHome)) {
            return Paths.get(dorisHome, "log").toString();
        }
        return Paths.get("log").toString();
    }

    private String normalizeReductionMode(String reductionMode) {
        if (Strings.isNullOrEmpty(reductionMode)) {
            return REDUCTION_GROUPED;
        }
        String mode = reductionMode.trim().toLowerCase(Locale.ROOT);
        if (!REDUCTION_GROUPED.equals(mode) && !REDUCTION_RAW.equals(mode)) {
            throw new IllegalArgumentException("Unsupported reductionMode: " + reductionMode);
        }
        return mode;
    }

    private int clamp(Integer value, int defaultValue, int minValue, int maxValue) {
        if (value == null) {
            return defaultValue;
        }
        return Math.min(maxValue, Math.max(minValue, value));
    }

    private List<String> normalizeNodes(List<String> nodes) {
        if (nodes == null) {
            return Collections.emptyList();
        }
        return nodes.stream().filter(Objects::nonNull).map(String::trim).filter(node -> !node.isEmpty())
                .distinct().collect(Collectors.toList());
    }

    private String trimText(String text) {
        if (text == null) {
            return null;
        }
        return text.length() <= MAX_EVENT_TEXT_LENGTH ? text : text.substring(0, MAX_EVENT_TEXT_LENGTH) + "...";
    }

    public String getCurrentFrontendNode() {
        HostInfo selfNode = org.apache.doris.catalog.Env.getCurrentEnv().getSelfNode();
        return NetUtils.getHostPortInAccessibleFormat(selfNode.getHost(), Config.http_port);
    }

    enum LogType {
        FE_INFO("fe.info", true, false, Arrays.asList("fe.log")),
        FE_WARN("fe.warn", true, false, Arrays.asList("fe.warn.log")),
        FE_GC("fe.gc", true, false, Arrays.asList("fe.gc.log")),
        BE_INFO("be.info", false, true, Arrays.asList("be.INFO.log")),
        BE_WARNING("be.warning", false, true, Arrays.asList("be.WARNING.log")),
        BE_GC("be.gc", false, true, Arrays.asList("be.gc.log")),
        BE_JNI("be.jni", false, true, Arrays.asList("jni.log"));

        private final String typeName;
        private final boolean frontend;
        private final boolean backend;
        private final List<String> prefixes;

        LogType(String typeName, boolean frontend, boolean backend, List<String> prefixes) {
            this.typeName = typeName;
            this.frontend = frontend;
            this.backend = backend;
            this.prefixes = prefixes;
        }

        boolean isFrontend() {
            return frontend;
        }

        boolean isBackend() {
            return backend;
        }

        boolean matches(String fileName) {
            return prefixes.stream().anyMatch(fileName::startsWith);
        }

        static Optional<LogType> fromName(String name) {
            return Arrays.stream(values()).filter(value -> value.typeName.equalsIgnoreCase(name)).findFirst();
        }
    }

    @Data
    public static class QueryRequest {
        private List<String> frontendNodes;
        private List<String> backendNodes;
        private List<String> logTypes;
        private Long startTimeMs;
        private Long endTimeMs;
        private String keyword;
        private String reductionMode;
        private Integer maxEntries;
        private Integer maxBytesPerNode;

        public QueryRequest copy() {
            QueryRequest copy = new QueryRequest();
            copy.setFrontendNodes(frontendNodes == null ? null : new ArrayList<>(frontendNodes));
            copy.setBackendNodes(backendNodes == null ? null : new ArrayList<>(backendNodes));
            copy.setLogTypes(logTypes == null ? null : new ArrayList<>(logTypes));
            copy.setStartTimeMs(startTimeMs);
            copy.setEndTimeMs(endTimeMs);
            copy.setKeyword(keyword);
            copy.setReductionMode(reductionMode);
            copy.setMaxEntries(maxEntries);
            copy.setMaxBytesPerNode(maxBytesPerNode);
            return copy;
        }
    }

    @Data
    public static class QueryResponse {
        private QueryRequest request;
        private List<NodeQueryResult> results = new ArrayList<>();
        private List<NodeQueryError> errors = new ArrayList<>();
    }

    @Data
    public static class NodeQueryPayload {
        private List<NodeQueryResult> results = new ArrayList<>();

        public NodeQueryPayload() {
        }

        public NodeQueryPayload(List<NodeQueryResult> results) {
            this.results = results;
        }
    }

    @Data
    public static class NodeQueryError {
        private String node;
        private String nodeType;
        private String message;
    }

    @Data
    public static class NodeQueryResult {
        private String node;
        private String nodeType;
        private String logType;
        private String reductionMode;
        private List<String> scannedFiles;
        private Integer matchedEventCount = 0;
        private Integer returnedItemCount = 0;
        private Boolean truncated = false;
        private String error;
        private List<LogGroupView> groups = Collections.emptyList();
        private List<LogEventView> events = Collections.emptyList();
    }

    @Data
    public static class LogGroupView {
        private String pattern;
        private int count;
        private Long firstTimeMs;
        private Long lastTimeMs;
        private List<String> examples;
    }

    @Data
    public static class LogEventView {
        private Long timeMs;
        private String firstLine;
        private String message;
    }

    @Data
    static class ParsedEvent {
        private Long timeMs;
        private String firstLine;
        private final StringBuilder messageBuilder = new StringBuilder();
        private String message;

        void append(String line) {
            if (messageBuilder.length() > 0) {
                messageBuilder.append('\n');
            }
            messageBuilder.append(line);
        }

        ParsedEvent finish() {
            message = messageBuilder.toString();
            return this;
        }

        long getSortTimeMs() {
            return timeMs == null ? Long.MIN_VALUE : timeMs;
        }
    }

    private class MutableGroup {
        private final String pattern;
        private int count;
        private Long firstTimeMs;
        private Long lastTimeMs;
        private final List<String> examples = new ArrayList<>();

        private MutableGroup(String pattern) {
            this.pattern = pattern;
        }

        private void add(ParsedEvent event) {
            count++;
            if (event.getTimeMs() != null) {
                if (firstTimeMs == null || event.getTimeMs() < firstTimeMs) {
                    firstTimeMs = event.getTimeMs();
                }
                if (lastTimeMs == null || event.getTimeMs() > lastTimeMs) {
                    lastTimeMs = event.getTimeMs();
                }
            }
            if (examples.size() < MAX_EXAMPLES_PER_GROUP) {
                examples.add(trimText(event.getMessage()));
            }
        }

        private int getCount() {
            return count;
        }

        private long getLastTimeMs() {
            return lastTimeMs == null ? Long.MIN_VALUE : lastTimeMs;
        }

        private LogGroupView toView() {
            LogGroupView view = new LogGroupView();
            view.setPattern(pattern);
            view.setCount(count);
            view.setFirstTimeMs(firstTimeMs);
            view.setLastTimeMs(lastTimeMs);
            view.setExamples(examples);
            return view;
        }
    }
}
