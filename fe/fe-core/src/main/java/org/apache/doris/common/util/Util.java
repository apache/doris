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

package org.apache.doris.common.util;

import org.apache.doris.catalog.Column;
import org.apache.doris.catalog.PrimitiveType;
import org.apache.doris.cloud.security.SecurityChecker;
import org.apache.doris.common.AnalysisException;
import org.apache.doris.common.FeNameFormat;
import org.apache.doris.datasource.InternalCatalog;
import org.apache.doris.qe.ConnectContext;
import org.apache.doris.thrift.TFileCompressType;
import org.apache.doris.thrift.TFileFormatType;

import com.google.common.base.Preconditions;
import com.google.common.base.Strings;
import com.google.common.collect.Lists;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.jetbrains.annotations.NotNull;

import java.io.BufferedReader;
import java.io.DataInput;
import java.io.DataOutput;
import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.PrintWriter;
import java.io.StringWriter;
import java.net.URL;
import java.net.URLConnection;
import java.nio.ByteBuffer;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.security.SecureRandom;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.function.LongUnaryOperator;
import java.util.function.Predicate;

public class Util {
    private static final Logger LOG = LogManager.getLogger(Util.class);
    private static final Map<PrimitiveType, String> TYPE_STRING_MAP = new HashMap<PrimitiveType, String>();

    private static final long DEFAULT_EXEC_CMD_TIMEOUT_MS = 600000L;

    private static final String[] ORDINAL_SUFFIX
            = new String[] { "th", "st", "nd", "rd", "th", "th", "th", "th", "th", "th" };

    private static final List<String> REGEX_ESCAPES
            = Lists.newArrayList("\\", "$", "(", ")", "*", "+", ".", "[", "]", "?", "^", "{", "}", "|");

    static {
        TYPE_STRING_MAP.put(PrimitiveType.TINYINT, "tinyint(4)");
        TYPE_STRING_MAP.put(PrimitiveType.SMALLINT, "smallint(6)");
        TYPE_STRING_MAP.put(PrimitiveType.INT, "int(11)");
        TYPE_STRING_MAP.put(PrimitiveType.BIGINT, "bigint(20)");
        TYPE_STRING_MAP.put(PrimitiveType.LARGEINT, "largeint(40)");
        TYPE_STRING_MAP.put(PrimitiveType.FLOAT, "float");
        TYPE_STRING_MAP.put(PrimitiveType.DOUBLE, "double");
        TYPE_STRING_MAP.put(PrimitiveType.DATE, "date");
        TYPE_STRING_MAP.put(PrimitiveType.DATETIME, "datetime");
        TYPE_STRING_MAP.put(PrimitiveType.DATEV2, "datev2");
        TYPE_STRING_MAP.put(PrimitiveType.DATETIMEV2, "datetimev2");
        TYPE_STRING_MAP.put(PrimitiveType.CHAR, "char(%d)");
        TYPE_STRING_MAP.put(PrimitiveType.VARCHAR, "varchar(%d)");
        TYPE_STRING_MAP.put(PrimitiveType.JSONB, "json");
        TYPE_STRING_MAP.put(PrimitiveType.STRING, "string");
        TYPE_STRING_MAP.put(PrimitiveType.DECIMALV2, "decimal(%d, %d)");
        TYPE_STRING_MAP.put(PrimitiveType.DECIMAL32, "decimal(%d, %d)");
        TYPE_STRING_MAP.put(PrimitiveType.DECIMAL64, "decimal(%d, %d)");
        TYPE_STRING_MAP.put(PrimitiveType.DECIMAL128, "decimal(%d, %d)");
        TYPE_STRING_MAP.put(PrimitiveType.DECIMAL256, "decimal(%d, %d)");
        TYPE_STRING_MAP.put(PrimitiveType.HLL, "varchar(%d)");
        TYPE_STRING_MAP.put(PrimitiveType.BOOLEAN, "bool");
        TYPE_STRING_MAP.put(PrimitiveType.BITMAP, "bitmap");
        TYPE_STRING_MAP.put(PrimitiveType.QUANTILE_STATE, "quantile_state");
        TYPE_STRING_MAP.put(PrimitiveType.AGG_STATE, "agg_state");
        TYPE_STRING_MAP.put(PrimitiveType.ARRAY, "array<%s>");
        TYPE_STRING_MAP.put(PrimitiveType.VARIANT, "variant");
        TYPE_STRING_MAP.put(PrimitiveType.NULL_TYPE, "null");
    }

    public static LongUnaryOperator overflowSafeIncrement() {
        return original -> {
            if (original == Long.MAX_VALUE) {
                return Long.MAX_VALUE;
            }
            long r = original + 1;
            if (r == Long.MAX_VALUE || ((original ^ r) & (1 ^ r)) < 0) {
                // unbounded reached
                return Long.MAX_VALUE;
            } else {
                return r;
            }
        };
    }

    private static class CmdWorker extends Thread {
        private final Process process;
        private Integer exitValue;

        private StringBuffer outBuffer;
        private StringBuffer errBuffer;

        public CmdWorker(final Process process) {
            this.process = process;
            this.outBuffer = new StringBuffer();
            this.errBuffer = new StringBuffer();
        }

        public Integer getExitValue() {
            return exitValue;
        }

        public String getStdOut() {
            return this.outBuffer.toString();
        }

        public String getErrOut() {
            return this.errBuffer.toString();
        }

        @Override
        public void run() {
            BufferedReader outReader = null;
            BufferedReader errReader = null;
            String line = null;
            try {
                outReader = new BufferedReader(new InputStreamReader(process.getInputStream()));
                while ((line = outReader.readLine()) != null) {
                    outBuffer.append(line + '\n');
                }

                errReader = new BufferedReader(new InputStreamReader(process.getErrorStream()));
                while ((line = errReader.readLine()) != null) {
                    errBuffer.append(line + '\n');
                }

                exitValue = process.waitFor();
            } catch (InterruptedException e) {
                LOG.warn("get exception", e);
            } catch (IOException e) {
                LOG.warn("get exception", e);
            } finally {
                try {
                    if (outReader != null) {
                        outReader.close();
                    }
                    if (errReader != null) {
                        errReader.close();
                    }
                } catch (IOException e) {
                    LOG.warn("close buffered reader error", e);
                }
            }
        }
    }

    public static CommandResult executeCommand(String cmd, String[] envp) {
        return executeCommand(cmd, envp, DEFAULT_EXEC_CMD_TIMEOUT_MS);
    }

    public static CommandResult executeCommand(String cmd, String[] envp, long timeoutMs) {
        CommandResult result = new CommandResult();
        List<String> cmdList = shellSplit(cmd);
        String[] cmds = cmdList.toArray(new String[0]);

        try {
            Process p = Runtime.getRuntime().exec(cmds, envp);
            CmdWorker cmdWorker = new CmdWorker(p);
            cmdWorker.start();

            Integer exitValue = -1;
            try {
                cmdWorker.join(timeoutMs);
                exitValue = cmdWorker.getExitValue();
                if (exitValue == null) {
                    // if we get this far then we never got an exit value from the worker thread
                    // as a result of a timeout
                    LOG.warn("exec command [{}] timed out.", cmd);
                    exitValue = -1;
                }
            } catch (InterruptedException ex) {
                cmdWorker.interrupt();
                Thread.currentThread().interrupt();
                throw ex;
            } finally {
                p.destroy();
            }

            result.setReturnCode(exitValue);
            result.setStdout(cmdWorker.getStdOut());
            result.setStderr(cmdWorker.getErrOut());
        } catch (IOException e) {
            LOG.warn("execute command error", e);
        } catch (InterruptedException e) {
            LOG.warn("execute command error", e);
        }

        return result;
    }

    public static List<String> shellSplit(CharSequence string) {
        List<String> tokens = new ArrayList<String>();
        boolean escaping = false;
        char quoteChar = ' ';
        boolean quoting = false;
        StringBuilder current = new StringBuilder();
        for (int i = 0; i < string.length(); i++) {
            char c = string.charAt(i);
            if (escaping) {
                current.append(c);
                escaping = false;
            } else if (c == '\\' && !(quoting && quoteChar == '\'')) {
                escaping = true;
            } else if (quoting && c == quoteChar) {
                quoting = false;
            } else if (!quoting && (c == '\'' || c == '"')) {
                quoting = true;
                quoteChar = c;
            } else if (!quoting && Character.isWhitespace(c)) {
                if (current.length() > 0) {
                    tokens.add(current.toString());
                    current = new StringBuilder();
                }
            } else {
                current.append(c);
            }
        }
        if (current.length() > 0) {
            tokens.add(current.toString());
        }
        return tokens;
    }

    // Get a string represent the schema signature, contains:
    // list of columns and bloom filter column info.
    public static String getSchemaSignatureString(List<Column> columns) {
        StringBuilder sb = new StringBuilder();
        for (Column column : columns) {
            sb.append(column.getSignatureString(TYPE_STRING_MAP));
        }
        return sb.toString();
    }

    public static int generateSchemaHash() {
        return Math.abs(new SecureRandom().nextInt());
    }

    /**
     * Chooses k unique random elements from a population sequence
     */
    public static <T> List<T> sample(List<T> population, int kNum) {
        if (population.isEmpty() || population.size() < kNum) {
            return null;
        }

        Collections.shuffle(population);
        return population.subList(0, kNum);
    }

    /**
     * Delete directory and all contents in this directory
     */
    public static boolean deleteDirectory(File directory) {
        if (!directory.exists()) {
            return true;
        }

        if (directory.isDirectory()) {
            File[] files = directory.listFiles();
            if (null != files) {
                for (File file : files) {
                    if (file.isDirectory()) {
                        deleteDirectory(file);
                    } else {
                        file.delete();
                    }
                }
            }
        }
        return directory.delete();
    }

    public static String dumpThread(Thread t, int lineNum) {
        StringBuilder sb = new StringBuilder();
        StackTraceElement[] elements = t.getStackTrace();
        sb.append("dump thread: ").append(t.getName()).append(", id: ").append(t.getId()).append("\n");
        int count = lineNum;
        for (StackTraceElement element : elements) {
            if (count == 0) {
                break;
            }
            sb.append("    ").append(element.toString()).append("\n");
            --count;
        }
        return sb.toString();
    }

    // get response body as a string from the given url.
    // "encodedAuthInfo", the base64 encoded auth info. like:
    //      Base64.encodeBase64String("user:passwd".getBytes());
    // If no auth info, pass a null.
    public static String getResultForUrl(String urlStr, String encodedAuthInfo, int connectTimeoutMs,
            int readTimeoutMs) {
        StringBuilder sb = new StringBuilder();
        InputStream stream = null;
        try {
            SecurityChecker.getInstance().startSSRFChecking(urlStr);
            URL url = new URL(urlStr);
            URLConnection conn = url.openConnection();
            if (encodedAuthInfo != null) {
                conn.setRequestProperty("Authorization", "Basic " + encodedAuthInfo);
            }
            conn.setConnectTimeout(connectTimeoutMs);
            conn.setReadTimeout(readTimeoutMs);

            stream = (InputStream) conn.getContent();
            BufferedReader br = new BufferedReader(new InputStreamReader(stream));

            String line;
            while ((line = br.readLine()) != null) {
                sb.append(line);
            }
        } catch (Exception e) {
            LOG.warn("failed to get result from url: {}. {}", urlStr, e.getMessage());
            return null;
        } finally {
            if (stream != null) {
                try {
                    stream.close();
                } catch (IOException e) {
                    LOG.warn("failed to close stream when get result from url: {}", urlStr, e);
                    return null;
                }
            }
            SecurityChecker.getInstance().stopSSRFChecking();
        }
        if (LOG.isDebugEnabled()) {
            LOG.debug("get result from url {}: {}", urlStr, sb.toString());
        }
        return sb.toString();
    }

    public static long getLongPropertyOrDefault(String valStr, long defaultVal, Predicate<Long> pred,
            String hintMsg) throws AnalysisException {
        if (Strings.isNullOrEmpty(valStr)) {
            return defaultVal;
        }

        long result = defaultVal;
        try {
            result = Long.valueOf(valStr);
        } catch (NumberFormatException e) {
            throw new AnalysisException(hintMsg);
        }

        if (pred == null) {
            return result;
        }

        if (!pred.test(result)) {
            throw new AnalysisException(hintMsg);
        }

        return result;
    }

    public static double getDoublePropertyOrDefault(String valStr, double defaultVal, Predicate<Double> pred,
                                                String hintMsg) throws AnalysisException {
        if (Strings.isNullOrEmpty(valStr)) {
            return defaultVal;
        }

        double result = defaultVal;
        try {
            result = Double.parseDouble(valStr);
        } catch (NumberFormatException e) {
            throw new AnalysisException(hintMsg);
        }

        if (pred == null) {
            return result;
        }

        if (!pred.test(result)) {
            throw new AnalysisException(hintMsg);
        }

        return result;
    }

    public static boolean getBooleanPropertyOrDefault(String valStr, boolean defaultVal, String hintMsg)
            throws AnalysisException {
        if (Strings.isNullOrEmpty(valStr)) {
            return defaultVal;
        }

        try {
            return Boolean.valueOf(valStr);
        } catch (NumberFormatException e) {
            throw new AnalysisException(hintMsg);
        }
    }

    // not support encode negative value now
    public static void encodeVarint64(long source, DataOutput out) throws IOException {
        assert source >= 0;
        short B = 128; // CHECKSTYLE IGNORE THIS LINE

        while (source > B) {
            out.write((int) (source & (B - 1) | B));
            source = source >> 7;
        }
        out.write((int) (source & (B - 1)));
    }

    // not support decode negative value now
    public static long decodeVarint64(DataInput in) throws IOException {
        long result = 0;
        int shift = 0;
        short B = 128; // CHECKSTYLE IGNORE THIS LINE

        while (true) {
            int oneByte = in.readUnsignedByte();
            boolean isEnd = (oneByte & B) == 0;
            result = result | ((long) (oneByte & B - 1) << (shift * 7));
            if (isEnd) {
                break;
            }
            shift++;
        }

        return result;
    }

    // return the ordinal string of an Integer
    public static String ordinal(int i) {
        switch (i % 100) {
            case 11:
            case 12:
            case 13:
                return i + "th";
            default:
                return i + ORDINAL_SUFFIX[i % 10];
        }
    }

    // get an input stream from url, the caller is responsible for closing the stream
    // "encodedAuthInfo", the base64 encoded auth info. like:
    //      Base64.encodeBase64String("user:passwd".getBytes());
    // If no auth info, pass a null.
    public static InputStream getInputStreamFromUrl(String urlStr, String encodedAuthInfo, int connectTimeoutMs,
            int readTimeoutMs) throws IOException {
        boolean needSecurityCheck = !(urlStr.startsWith("/") || urlStr.startsWith("file://"));
        try {
            if (needSecurityCheck) {
                SecurityChecker.getInstance().startSSRFChecking(urlStr);
            }
            URL url = new URL(urlStr);
            URLConnection conn = url.openConnection();
            if (encodedAuthInfo != null) {
                conn.setRequestProperty("Authorization", "Basic " + encodedAuthInfo);
            }
            conn.setConnectTimeout(connectTimeoutMs);
            conn.setReadTimeout(readTimeoutMs);
            return conn.getInputStream();
        } catch (Exception e) {
            throw new IOException(e);
        } finally {
            if (needSecurityCheck) {
                SecurityChecker.getInstance().stopSSRFChecking();
            }
        }
    }

    public static boolean showHiddenColumns() {
        return ConnectContext.get() != null && (
            ConnectContext.get().getSessionVariable().showHiddenColumns()
            || ConnectContext.get().getSessionVariable().skipStorageEngineMerge());
    }

    public static String escapeSingleRegex(String s) {
        Preconditions.checkArgument(s.length() == 1);
        if (REGEX_ESCAPES.contains(s)) {
            return "\\" + s;
        }
        return s;
    }

    /**
     * Check all rules of catalog.
     */
    public static void checkCatalogAllRules(String catalog) throws AnalysisException {
        if (Strings.isNullOrEmpty(catalog)) {
            throw new AnalysisException("Catalog name is empty.");
        }

        if (!catalog.equals(InternalCatalog.INTERNAL_CATALOG_NAME)) {
            FeNameFormat.checkCommonName("catalog", catalog);
        }
    }

    public static void prohibitExternalCatalog(String catalog, String msg) throws AnalysisException {
        if (!Strings.isNullOrEmpty(catalog) && !catalog.equals(InternalCatalog.INTERNAL_CATALOG_NAME)) {
            throw new AnalysisException(String.format("External catalog '%s' is not allowed in '%s'", catalog, msg));
        }
    }

    private static final char[] HEX_ARRAY = "0123456789ABCDEF".toCharArray();

    public static String bytesToHex(byte[] bytes) {
        char[] hexChars = new char[bytes.length * 2];
        for (int j = 0; j < bytes.length; j++) {
            int v = bytes[j] & 0xFF;
            hexChars[j * 2] = HEX_ARRAY[v >>> 4];
            hexChars[j * 2 + 1] = HEX_ARRAY[v & 0x0F];
        }
        return new String(hexChars);
    }


    @NotNull
    public static TFileFormatType getFileFormatTypeFromPath(String path) {
        String lowerCasePath = path.toLowerCase();
        if (lowerCasePath.contains(".parquet") || lowerCasePath.contains(".parq")) {
            return TFileFormatType.FORMAT_PARQUET;
        } else if (lowerCasePath.contains(".orc")) {
            return TFileFormatType.FORMAT_ORC;
        } else if (lowerCasePath.contains(".json")) {
            return TFileFormatType.FORMAT_JSON;
        } else {
            return TFileFormatType.FORMAT_CSV_PLAIN;
        }
    }

    public static TFileFormatType getFileFormatTypeFromName(String formatName) {
        String lowerFileFormat = Objects.requireNonNull(formatName).toLowerCase();
        if (lowerFileFormat.equals(FileFormatConstants.FORMAT_PARQUET)) {
            return TFileFormatType.FORMAT_PARQUET;
        } else if (lowerFileFormat.equals(FileFormatConstants.FORMAT_ORC)) {
            return TFileFormatType.FORMAT_ORC;
        } else if (lowerFileFormat.equals(FileFormatConstants.FORMAT_JSON)) {
            return TFileFormatType.FORMAT_JSON;
            // csv/csv_with_name/csv_with_names_and_types treat as csv format
        } else if (lowerFileFormat.equals(FileFormatConstants.FORMAT_CSV)
                || lowerFileFormat.equals(FileFormatConstants.FORMAT_CSV_WITH_NAMES)
                || lowerFileFormat.equals(FileFormatConstants.FORMAT_CSV_WITH_NAMES_AND_TYPES)
                // TODO: Add TEXTFILE to TFileFormatType to Support hive text file format.
                || lowerFileFormat.equals(FileFormatConstants.FORMAT_HIVE_TEXT)) {
            return TFileFormatType.FORMAT_CSV_PLAIN;
        } else if (lowerFileFormat.equals(FileFormatConstants.FORMAT_WAL)) {
            return TFileFormatType.FORMAT_WAL;
        } else if (lowerFileFormat.equals(FileFormatConstants.FORMAT_ARROW)) {
            return TFileFormatType.FORMAT_ARROW;
        } else {
            return TFileFormatType.FORMAT_UNKNOWN;
        }
    }

    /**
     * Infer {@link TFileCompressType} from file name.
     *
     * @param path of file to be inferred.
     */
    @NotNull
    public static TFileCompressType inferFileCompressTypeByPath(String path) {
        String lowerCasePath = path.toLowerCase();
        if (lowerCasePath.endsWith(".gz")) {
            return TFileCompressType.GZ;
        } else if (lowerCasePath.endsWith(".bz2")) {
            return TFileCompressType.BZ2;
        } else if (lowerCasePath.endsWith(".lz4")) {
            return TFileCompressType.LZ4FRAME;
        } else if (lowerCasePath.endsWith(".lzo")) {
            return TFileCompressType.LZOP;
        } else if (lowerCasePath.endsWith(".lzo_deflate")) {
            return TFileCompressType.LZO;
        } else if (lowerCasePath.endsWith(".deflate")) {
            return TFileCompressType.DEFLATE;
        } else if (lowerCasePath.endsWith(".snappy")) {
            return TFileCompressType.SNAPPYBLOCK;
        } else {
            return TFileCompressType.PLAIN;
        }
    }

    public static TFileCompressType getFileCompressType(String compressType) {
        if (Strings.isNullOrEmpty(compressType)) {
            return TFileCompressType.UNKNOWN;
        }
        final String upperCaseType = compressType.toUpperCase();
        return TFileCompressType.valueOf(upperCaseType);
    }

    /**
     * Pass through the compressType if it is not {@link TFileCompressType#UNKNOWN}. Otherwise, return the
     * inferred type from path.
     */
    public static TFileCompressType getOrInferCompressType(TFileCompressType compressType, String path) {
        return compressType == TFileCompressType.UNKNOWN
                ? inferFileCompressTypeByPath(path.toLowerCase()) : compressType;
    }

    public static boolean isCsvFormat(TFileFormatType fileFormatType) {
        return fileFormatType == TFileFormatType.FORMAT_CSV_BZ2
                || fileFormatType == TFileFormatType.FORMAT_CSV_DEFLATE
                || fileFormatType == TFileFormatType.FORMAT_CSV_GZ
                || fileFormatType == TFileFormatType.FORMAT_CSV_LZ4FRAME
                || fileFormatType == TFileFormatType.FORMAT_CSV_LZ4BLOCK
                || fileFormatType == TFileFormatType.FORMAT_CSV_SNAPPYBLOCK
                || fileFormatType == TFileFormatType.FORMAT_CSV_LZO
                || fileFormatType == TFileFormatType.FORMAT_CSV_LZOP
                || fileFormatType == TFileFormatType.FORMAT_CSV_PLAIN;
    }

    public static void logAndThrowRuntimeException(Logger logger, String msg, Throwable e) {
        logger.warn(msg, e);
        throw new RuntimeException(msg, e);
    }

    public static String getRootCauseMessage(Throwable t) {
        String rootCause = "unknown";
        Throwable p = t;
        while (p != null) {
            rootCause = p.getClass().getName() + ": " + p.getMessage();
            p = p.getCause();
        }
        return rootCause;
    }

    // Return the stack of the root cause
    public static String getRootCauseStack(Throwable t) {
        String rootStack = "unknown";
        if (t == null) {
            return rootStack;
        }
        Throwable p = t;
        while (p.getCause() != null) {
            p = p.getCause();
        }
        StringWriter sw = new StringWriter();
        PrintWriter pw = new PrintWriter(sw);
        p.printStackTrace(pw);
        return sw.toString();
    }

    public static long sha256long(String str) {
        try {
            MessageDigest digest = MessageDigest.getInstance("SHA-256");
            byte[] hash = digest.digest(str.getBytes());
            ByteBuffer buffer = ByteBuffer.wrap(hash);
            return buffer.getLong();
        } catch (NoSuchAlgorithmException e) {
            return str.hashCode();
        }
    }

    // Only used for external table's id generation
    // And the table's id must >=0, see DescriptorTable.toThrift()
    public static long genTableIdByName(String tblName) {
        return Math.abs(sha256long(tblName));
    }
}
