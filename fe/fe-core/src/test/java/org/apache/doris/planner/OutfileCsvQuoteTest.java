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

package org.apache.doris.planner;

import org.apache.doris.analysis.BrokerDesc;
import org.apache.doris.analysis.OutFileClause;
import org.apache.doris.analysis.StorageBackend;
import org.apache.doris.common.Config;
import org.apache.doris.common.UserException;
import org.apache.doris.common.io.Text;
import org.apache.doris.datasource.property.fileformat.CsvFileFormatProperties;
import org.apache.doris.load.ExportJob;
import org.apache.doris.nereids.trees.plans.commands.ExportCommand;
import org.apache.doris.persist.gson.GsonUtils;
import org.apache.doris.thrift.TResultFileSinkOptions;

import com.google.gson.JsonObject;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

public class OutfileCsvQuoteTest {
    private boolean oldEnableOutfileToLocal;

    @BeforeEach
    public void setUp() {
        oldEnableOutfileToLocal = Config.enable_outfile_to_local;
        Config.enable_outfile_to_local = true;
    }

    @AfterEach
    public void tearDown() {
        Config.enable_outfile_to_local = oldEnableOutfileToLocal;
    }

    @Test
    public void testOutfileThriftAndQuotedHeader() throws UserException {
        Map<String, String> properties = new HashMap<>();
        properties.put(CsvFileFormatProperties.PROP_CSV_QUOTE, "true");
        properties.put(CsvFileFormatProperties.PROP_COLUMN_SEPARATOR, "||");
        properties.put(CsvFileFormatProperties.PROP_LINE_DELIMITER, "\r\n");
        OutFileClause clause = new OutFileClause("file:///tmp/outfile-csv-quote", "csv_with_names",
                properties);
        clause.analyze(Collections.emptyList(), Collections.emptyList(), false);

        TResultFileSinkOptions sinkOptions = clause.toSinkOptions();
        Assertions.assertTrue(sinkOptions.isCsvQuote());
        Assertions.assertEquals("\"a||b\"||\"say \"\"hi\"\"\"||\"\"\r\n",
                ResultFileSink.genNames(
                        new ArrayList<>(java.util.Arrays.asList("a||b", "say \"hi\"", "")),
                        "||", "\r\n", true));
    }

    @Test
    public void testUnquotedHeaderRemainsByteCompatible() {
        Assertions.assertEquals("a,b\n",
                ResultFileSink.genNames(
                        new ArrayList<>(java.util.Arrays.asList("a", "b")), ",", "\n", false));
    }

    @Test
    public void testCsvQuoteRejectedForNonCsvOutfile() {
        Map<String, String> properties = new HashMap<>();
        properties.put(CsvFileFormatProperties.PROP_CSV_QUOTE, "true");
        OutFileClause clause = new OutFileClause("file:///tmp/outfile-csv-quote", "parquet", properties);

        Exception exception = Assertions.assertThrows(Exception.class,
                () -> clause.analyze(Collections.emptyList(), Collections.emptyList(), false));
        Assertions.assertTrue(exception.getMessage().contains("csv_quote is only supported for CSV format"));
    }

    @SuppressWarnings("unchecked")
    @Test
    public void testExportJobPersistsAndForwardsCsvQuote() throws Exception {
        ExportJob job = new ExportJob(1);
        job.setFormat("csv");
        job.setColumnSeparator(",");
        job.setLineDelimiter("\n");
        job.setCsvQuote("true");
        job.setMaxFileSize("");
        job.setWithBom("false");
        job.setBrokerDesc(new BrokerDesc("local", StorageBackend.StorageType.LOCAL, null));

        ByteArrayOutputStream bytes = new ByteArrayOutputStream();
        job.write(new DataOutputStream(bytes));
        ExportJob restored = ExportJob.read(
                new DataInputStream(new ByteArrayInputStream(bytes.toByteArray())));

        Method method = ExportJob.class.getDeclaredMethod("convertOutfileProperties");
        method.setAccessible(true);
        Map<String, String> properties = (Map<String, String>) method.invoke(restored);

        Assertions.assertEquals("true", restored.getCsvQuote());
        Assertions.assertEquals("true", properties.get(CsvFileFormatProperties.PROP_CSV_QUOTE));
    }

    @SuppressWarnings("unchecked")
    @Test
    public void testOldExportJobWithoutCsvQuoteDefaultsFalse() throws Exception {
        ExportJob job = new ExportJob(2);
        job.setFormat("csv");
        job.setColumnSeparator(",");
        job.setLineDelimiter("\n");
        job.setMaxFileSize("");
        job.setWithBom("false");
        job.setBrokerDesc(new BrokerDesc("local", StorageBackend.StorageType.LOCAL, null));

        JsonObject oldJson = GsonUtils.GSON.toJsonTree(job).getAsJsonObject();
        oldJson.remove("csvQuote");
        ByteArrayOutputStream bytes = new ByteArrayOutputStream();
        Text.writeString(new DataOutputStream(bytes), oldJson.toString());
        ExportJob restored = ExportJob.read(
                new DataInputStream(new ByteArrayInputStream(bytes.toByteArray())));

        Method method = ExportJob.class.getDeclaredMethod("convertOutfileProperties");
        method.setAccessible(true);
        Map<String, String> properties = (Map<String, String>) method.invoke(restored);

        Assertions.assertEquals("false", restored.getCsvQuote());
        Assertions.assertEquals("false", properties.get(CsvFileFormatProperties.PROP_CSV_QUOTE));
    }

    @Test
    public void testExportRejectsInvalidCsvQuoteAndNonCsvFormat() throws Exception {
        ExportCommand command = new ExportCommand(
                java.util.Arrays.asList("catalog", "db", "table"),
                Collections.emptyList(), java.util.Optional.empty(), "/tmp/export",
                Collections.emptyMap(), java.util.Optional.empty());
        Method method = ExportCommand.class.getDeclaredMethod(
                "checkFileProperties", org.apache.doris.qe.ConnectContext.class,
                Map.class, org.apache.doris.catalog.info.TableNameInfo.class);
        method.setAccessible(true);

        Map<String, String> invalidBoolean = new HashMap<>();
        invalidBoolean.put(CsvFileFormatProperties.PROP_CSV_QUOTE, "1");
        InvocationTargetException invalidBooleanError = Assertions.assertThrows(
                InvocationTargetException.class,
                () -> method.invoke(command, null, invalidBoolean, null));
        Assertions.assertTrue(invalidBooleanError.getCause().getMessage()
                .contains("csv_quote must be true or false"));

        Map<String, String> nonCsv = new HashMap<>();
        nonCsv.put(CsvFileFormatProperties.PROP_CSV_QUOTE, "true");
        nonCsv.put("format", "parquet");
        InvocationTargetException nonCsvError = Assertions.assertThrows(
                InvocationTargetException.class,
                () -> method.invoke(command, null, nonCsv, null));
        Assertions.assertTrue(nonCsvError.getCause().getMessage()
                .contains("csv_quote is only supported for CSV format"));
    }

    @Test
    public void testExportRejectsDoubleQuoteSeparatorsBeforeJobRegistration() throws Exception {
        for (String property : new String[] {
                CsvFileFormatProperties.PROP_COLUMN_SEPARATOR,
                CsvFileFormatProperties.PROP_LINE_DELIMITER}) {
            for (String separator : new String[] {"|\"", "\\x7c\\x22"}) {
                Map<String, String> properties = new HashMap<>();
                properties.put(CsvFileFormatProperties.PROP_CSV_QUOTE, "true");
                properties.put(property, separator);

                InvocationTargetException exception = Assertions.assertThrows(
                        InvocationTargetException.class,
                        () -> invokeExportFilePropertyCheck(properties));
                Assertions.assertTrue(exception.getCause().getMessage()
                        .contains("must not contain double quote when csv_quote is true"));
            }
        }
    }

    @Test
    public void testExportAcceptsCompatibleCsvQuoteSeparators() throws Exception {
        invokeExportFilePropertyCheck(Collections.emptyMap());

        Map<String, String> properties = new HashMap<>();
        properties.put(CsvFileFormatProperties.PROP_CSV_QUOTE, "true");
        properties.put(CsvFileFormatProperties.PROP_COLUMN_SEPARATOR, "\\x7c\\x7c");
        properties.put(CsvFileFormatProperties.PROP_LINE_DELIMITER, "\\r\\n");
        invokeExportFilePropertyCheck(properties);
    }

    @Test
    public void testExportDisabledCsvQuoteKeepsLegacySeparatorRules() throws Exception {
        Map<String, String> properties = new HashMap<>();
        properties.put(CsvFileFormatProperties.PROP_CSV_QUOTE, "false");
        properties.put(CsvFileFormatProperties.PROP_COLUMN_SEPARATOR, "|\"");
        properties.put(CsvFileFormatProperties.PROP_LINE_DELIMITER, "\\x22\\x0a");
        invokeExportFilePropertyCheck(properties);
    }

    private void invokeExportFilePropertyCheck(Map<String, String> properties) throws Exception {
        ExportCommand command = new ExportCommand(
                java.util.Arrays.asList("catalog", "db", "table"),
                Collections.emptyList(), java.util.Optional.empty(), "/tmp/export",
                properties, java.util.Optional.empty());
        Method method = ExportCommand.class.getDeclaredMethod(
                "checkFileProperties", org.apache.doris.qe.ConnectContext.class,
                Map.class, org.apache.doris.catalog.info.TableNameInfo.class);
        method.setAccessible(true);
        method.invoke(command, null, properties, null);
    }
}
