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

package org.apache.doris.connector.lance;

import org.apache.doris.connector.spi.ConnectorColumn;
import org.apache.doris.connector.spi.ConnectorType;
import org.apache.doris.connector.spi.DorisConnectorException;
import org.apache.doris.connector.spi.handle.ConnectorColumnHandle;
import org.apache.doris.connector.spi.handle.ConnectorTableHandle;
import org.apache.doris.connector.spi.handle.NamedColumnHandle;
import org.apache.doris.connector.spi.pushdown.ConnectorColumnRef;
import org.apache.doris.connector.spi.pushdown.ConnectorComparison;
import org.apache.doris.connector.spi.pushdown.ConnectorExpression;
import org.apache.doris.connector.spi.pushdown.ConnectorLiteral;
import org.apache.doris.connector.spi.scan.ConnectorScanRange;
import org.apache.doris.connector.spi.scan.ConnectorScanRequest;
import org.apache.doris.connector.spi.scan.ScanNodePropertiesResult;
import org.apache.doris.connector.spi.scan.ScanNodePropertyKeys;
import org.apache.doris.connector.spi.search.ConnectorSearch;
import org.apache.doris.connector.spi.search.ConnectorSearchFunction;
import org.apache.doris.connector.spi.search.ConnectorSearchLazyMaterialization;
import org.apache.doris.connector.spi.search.ConnectorSearchRequest;
import org.apache.doris.thrift.TExternalSearchRequest;
import org.apache.doris.thrift.TFileRangeDesc;
import org.apache.doris.thrift.TFileScanRangeParams;
import org.apache.doris.thrift.TTableFormatFileDesc;

import org.apache.arrow.vector.types.FloatingPointPrecision;
import org.apache.arrow.vector.types.pojo.ArrowType;
import org.apache.arrow.vector.types.pojo.Field;
import org.apache.arrow.vector.types.pojo.FieldType;
import org.apache.arrow.vector.types.pojo.Schema;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.lance.index.IndexType;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.UUID;
import java.util.stream.Collectors;

/**
 * A search over a Lance catalog table, from its analysis to its scan: the version it reads and the columns it
 * returns, the field id a search that may use an index needs, when the engine may fetch its columns after the
 * merge, and what the scan sends the BE and shows in EXPLAIN. Each test runs in one statement whose memo already
 * holds the version, so nothing here opens a dataset.
 */
class LanceConnectorSearchTest {
    private static final String URI = "s3://warehouse/lance/items.lance";
    private static final LanceTableHandle TABLE = new LanceTableHandle("db", "items");
    private static final UUID SEGMENT = UUID.fromString("11111111-2222-3333-4444-555555555555");
    private static final Schema SCHEMA = new Schema(Arrays.asList(
            Field.nullable("id", new ArrowType.Int(64, true)),
            new Field("Embedding", FieldType.nullable(new ArrowType.FixedSizeList(2)), Collections.singletonList(
                    Field.nullable("item", new ArrowType.FloatingPoint(FloatingPointPrecision.SINGLE)))),
            Field.nullable("body", ArrowType.Utf8.INSTANCE),
            Field.nullable("null_col", ArrowType.Null.INSTANCE)));

    private final LanceTestSession session = new LanceTestSession();

    @Test
    void vectorSearchReadsTheStatementVersionAndReturnsTheTableColumnsAndDistance() {
        readInThisStatement(SCHEMA, indexes(Collections.singletonMap("Embedding", 9)));

        ConnectorSearch search = metadata().prepareSearch(session, TABLE, vector(map("column", "embedding",
                "query_vector", "[1, 2]", "top_k", "5", "offset", "2")));

        Assertions.assertEquals(Arrays.asList("id", "Embedding", "body", "null_col", "_distance"),
                search.getColumns().stream().map(ConnectorColumn::getName).collect(Collectors.toList()));
        ConnectorColumn distance = search.getColumns().get(4);
        Assertions.assertEquals("FLOAT", distance.getType().getTypeName());
        Assertions.assertTrue(distance.isNullable());
        Assertions.assertEquals(5, search.getTopK());
        Assertions.assertEquals(2, search.getOffset());
        Assertions.assertEquals(ConnectorSearchLazyMaterialization.TOP_LEVEL_COLUMNS, search.getLazyMaterialization());
        LanceSearchTableHandle handle = (LanceSearchTableHandle) search.getScanHandle();
        Assertions.assertEquals(TABLE.withVersion(3), handle.getTable());
        Assertions.assertEquals(9, handle.getSearchFieldId());
        Assertions.assertEquals("Embedding", handle.getRequest().getSearchQuery().getVectorSearch().getColumn());
        Assertions.assertEquals("db.items@3 vector_search(Embedding)", handle.toString());
    }

    @Test
    void fullTextSearchReturnsTheScoreColumn() {
        readInThisStatement(SCHEMA, indexes(Collections.singletonMap("body", 7)));

        ConnectorSearch search = metadata().prepareSearch(session, TABLE, fullText(map("column", "BODY",
                "query", "lance")));

        Assertions.assertEquals("_score", search.getColumns().get(search.getColumns().size() - 1).getName());
        Assertions.assertEquals(7, ((LanceSearchTableHandle) search.getScanHandle()).getSearchFieldId());
        Assertions.assertEquals(10, search.getTopK());
    }

    @Test
    void anInvalidArgumentFailsBeforeTheTableIsRead() {
        // Nothing is remembered for this statement and the client cannot open a dataset.
        DorisConnectorException error = Assertions.assertThrows(DorisConnectorException.class,
                () -> metadata().prepareSearch(session, TABLE, vector(map("column", "embedding",
                        "query_vector", "[1, 2]", "distance_upper_bound", "NaN"))));
        Assertions.assertEquals("'distance_upper_bound' must be a finite FLOAT", error.getMessage());
    }

    @Test
    void searchThatMayUseAnIndexNeedsTheFieldIdOfItsColumn() {
        readInThisStatement(SCHEMA, indexes(Collections.singletonMap("id", 1)));

        DorisConnectorException vector = Assertions.assertThrows(DorisConnectorException.class,
                () -> metadata().prepareSearch(session, TABLE, vector(map("column", "embedding",
                        "query_vector", "[1, 2]"))));
        Assertions.assertEquals("Lance vector column 'Embedding' has no field ID in the Lance schema",
                vector.getMessage());
        DorisConnectorException fullText = Assertions.assertThrows(DorisConnectorException.class,
                () -> metadata().prepareSearch(session, TABLE, fullText(map("column", "body", "query", "a"))));
        Assertions.assertEquals("Lance full-text column 'body' has no field ID in the Lance schema",
                fullText.getMessage());
    }

    @Test
    void vectorSearchWithoutIndexesReadsNoIndexMetadata() {
        // No index metadata is remembered, and the client cannot read any.
        readInThisStatement(SCHEMA, null);

        ConnectorSearch search = metadata().prepareSearch(session, TABLE, vector(map("column", "embedding",
                "query_vector", "[1, 2]", "use_index", "false")));

        Assertions.assertEquals(-1, ((LanceSearchTableHandle) search.getScanHandle()).getSearchFieldId());
        List<ConnectorScanRange> ranges = provider().planScan(session,
                ConnectorScanRequest.builder(search.getScanHandle(), Collections.emptyList()).build());
        Assertions.assertEquals(2, ranges.size());
    }

    @Test
    void theEngineFetchesColumnsAfterTheMergeOnlyWhenEnabledAndEveryBackendReadsRowLocations() {
        readInThisStatement(SCHEMA, indexes(Collections.singletonMap("Embedding", 9)));
        ConnectorSearchRequest request = vector(map("column", "embedding", "query_vector", "[1, 2]"));

        session.setBeExecVersion(LanceConnectorMetadata.LANCE_ROW_LOCATION_MIN_BE_EXEC_VERSION);
        Assertions.assertEquals(ConnectorSearchLazyMaterialization.TOP_LEVEL_COLUMNS,
                metadata().prepareSearch(session, TABLE, request).getLazyMaterialization());
        // An older backend would read the row location as another kind of row id: no lazy fetch, same rows.
        session.setBeExecVersion(LanceConnectorMetadata.LANCE_ROW_LOCATION_MIN_BE_EXEC_VERSION - 1);
        Assertions.assertEquals(ConnectorSearchLazyMaterialization.NONE,
                metadata().prepareSearch(session, TABLE, request).getLazyMaterialization());

        session.setBeExecVersion(Integer.MAX_VALUE);
        session.setSessionProperty(LanceConnectorMetadata.SESSION_ENABLE_LAZY_MATERIALIZATION, "false");
        Assertions.assertEquals(ConnectorSearchLazyMaterialization.NONE,
                metadata().prepareSearch(session, TABLE, request).getLazyMaterialization());
        session.setSessionProperty(LanceConnectorMetadata.SESSION_ENABLE_LAZY_MATERIALIZATION, "true");
        Assertions.assertEquals(ConnectorSearchLazyMaterialization.TOP_LEVEL_COLUMNS,
                metadata().prepareSearch(session, TABLE, request).getLazyMaterialization());
    }

    @Test
    void rejectsColumnsTheEngineCouldNotTellApart() {
        Schema duplicate = new Schema(Arrays.asList(Field.nullable("Category", ArrowType.Utf8.INSTANCE),
                Field.nullable("category", ArrowType.Utf8.INSTANCE), Field.nullable("body", ArrowType.Utf8.INSTANCE)));
        readInThisStatement(duplicate, indexes(Collections.singletonMap("body", 7)));
        DorisConnectorException error = Assertions.assertThrows(DorisConnectorException.class,
                () -> metadata().prepareSearch(session, TABLE, fullText(map("column", "body", "query", "a"))));
        Assertions.assertEquals("Duplicate Lance schema column under case-insensitive matching: 'category'",
                error.getMessage());
    }

    @Test
    void rejectsATableColumnNamedLikeTheResultColumn() {
        Schema scored = new Schema(Arrays.asList(Field.nullable("body", ArrowType.Utf8.INSTANCE),
                Field.nullable("_Score", new ArrowType.FloatingPoint(FloatingPointPrecision.SINGLE))));
        readInThisStatement(scored, indexes(Collections.singletonMap("body", 7)));
        DorisConnectorException error = Assertions.assertThrows(DorisConnectorException.class,
                () -> metadata().prepareSearch(session, TABLE, fullText(map("column", "body", "query", "a"))));
        Assertions.assertEquals("Lance table already contains reserved full-text search column '_score'",
                error.getMessage());
    }

    @Test
    void theColumnHandlesOfASearchIncludeItsResultColumn() {
        readInThisStatement(SCHEMA, indexes(Collections.singletonMap("Embedding", 9)));
        ConnectorSearch search = metadata().prepareSearch(session, TABLE, vector(map("column", "embedding",
                "query_vector", "[1, 2]")));

        Map<String, ConnectorColumnHandle> handles = metadata().getColumnHandles(session, search.getScanHandle());

        Assertions.assertEquals(Arrays.asList("id", "Embedding", "body", "null_col", "_distance"),
                new ArrayList<>(handles.keySet()));
        Assertions.assertEquals("_distance", ((NamedColumnHandle) handles.get("_distance")).getName());
        Assertions.assertEquals(Arrays.asList("id", "Embedding", "body", "null_col"),
                new ArrayList<>(metadata().getColumnHandles(session, TABLE.withVersion(3)).keySet()));
    }

    @Test
    void theScanSendsEachRangesRequestAndKeepsTheWhereInDoris() {
        readInThisStatement(SCHEMA, indexes(Collections.singletonMap("Embedding", 9)));
        ConnectorSearch search = metadata().prepareSearch(session, TABLE, vector(map("column", "embedding",
                "query_vector", "[1, 2]", "top_k", "5", "offset", "2", "filter", "id > 1", "prefilter", "true")));
        ConnectorExpression where = new ConnectorComparison(ConnectorComparison.Operator.GT,
                new ConnectorColumnRef("id", ConnectorType.of("BIGINT")), ConnectorLiteral.ofLong(3));

        ScanNodePropertiesResult result = provider().getScanNodePropertiesResult(session, search.getScanHandle(),
                Arrays.asList(new NamedColumnHandle("id"), new NamedColumnHandle("_distance")), Optional.of(where));

        // Nothing is pruned: the WHERE filters each range's candidates before the engine merges them.
        Assertions.assertFalse(result.hasConjunctTracking());
        Map<String, String> props = result.getProperties();
        Assertions.assertEquals("lance", props.get(ScanNodePropertyKeys.FILE_FORMAT_TYPE));
        Assertions.assertEquals("v", props.get(ScanNodePropertyKeys.LOCATION_PREFIX + "k"));
        Assertions.assertFalse(props.containsKey(LanceScanPlanProvider.PROP_SUBSTRAIT_FILTER));
        Assertions.assertFalse(props.containsKey(ScanNodePropertyKeys.REQUIRED_CURRENT_BACKEND_SEMANTICS));

        TFileScanRangeParams params = new TFileScanRangeParams();
        provider().populateScanLevelParams(params, props);
        TExternalSearchRequest sent = params.getLanceScanParams().getExternalSearchRequest();
        TExternalSearchRequest logical = ((LanceSearchTableHandle) search.getScanHandle()).getRequest();
        Assertions.assertEquals(((LanceSearchTableHandle) search.getScanHandle()).splitRequest(), sent);
        Assertions.assertEquals(7, sent.getSearchQuery().getVectorSearch().getTopK());
        Assertions.assertEquals(0, sent.getSearchQuery().getVectorSearch().getOffset());
        Assertions.assertTrue(sent.isPrefilter());
        Assertions.assertArrayEquals(logical.getSearchFilter().getPayload(), sent.getSearchFilter().getPayload());
        Assertions.assertEquals(Collections.singletonMap("aws_region", "us-east-1"),
                params.getLanceScanParams().getLanceStorageOptions());
        Assertions.assertFalse(params.getLanceScanParams().isSetLanceSubstraitFilter());

        StringBuilder explain = new StringBuilder();
        provider().appendExplainInfo(explain, "  ", props);
        Assertions.assertEquals("  externalSearchType=VECTOR\n"
                + "  lanceVectorIndexStatus=USED\n"
                + "  lanceVectorColumn=Embedding\n"
                + "  lanceMetric=l2\n"
                + "  lanceVersion=3\n"
                + "  lanceSearchFragments=2\n"
                + "  lanceSearchUnindexedFragments=1\n"
                + "  lanceSearchIndexSegments=1\n"
                + "  lanceSearchIndexFragments=1\n", explain.toString());
        Assertions.assertFalse(explain.toString().contains("id > 1"));

        // Every range searches its own candidates: neither the WHERE nor a LIMIT bounds it.
        List<ConnectorScanRange> ranges = provider().planScan(session, ConnectorScanRequest.builder(
                search.getScanHandle(), Collections.emptyList()).filter(Optional.of(where)).limit(1).build());
        Assertions.assertEquals(2, ranges.size());
        for (ConnectorScanRange range : ranges) {
            TTableFormatFileDesc formatDesc = new TTableFormatFileDesc();
            range.populateRangeParams(formatDesc, new TFileRangeDesc());
            Assertions.assertFalse(formatDesc.getLanceParams().isSetLimit());
            Assertions.assertEquals(3, formatDesc.getLanceParams().getVersion());
        }
    }

    @Test
    void columnReadAfterTheMergeKeepsAnOlderBackendFromServingTheSearch() {
        readInThisStatement(SCHEMA, indexes(Collections.singletonMap("Embedding", 9)));
        ConnectorTableHandle search = metadata().prepareSearch(session, TABLE, vector(map("column", "embedding",
                "query_vector", "[1, 2]"))).getScanHandle();
        List<ConnectorColumnHandle> scanned = Collections.singletonList(new NamedColumnHandle("id"));

        Assertions.assertFalse(provider().getScanNodePropertiesResult(session, search, scanned, Optional.empty())
                .getProperties().containsKey(ScanNodePropertyKeys.REQUIRED_CURRENT_BACKEND_SEMANTICS));
        ConnectorTableHandle lazy = metadata().applyTopnLazyMaterialization(session, search,
                Collections.singletonList("null_col"));
        Assertions.assertEquals(Collections.singletonList("null_col"),
                ((LanceSearchTableHandle) lazy).getLazyColumns());
        Assertions.assertEquals(LanceScanPlanProvider.ADDITIONAL_TYPES_SEMANTICS,
                provider().getScanNodePropertiesResult(session, lazy, scanned, Optional.empty())
                        .getProperties().get(ScanNodePropertyKeys.REQUIRED_CURRENT_BACKEND_SEMANTICS));
        // A plain table's handle is the connector's to keep.
        Assertions.assertSame(TABLE, metadata().applyTopnLazyMaterialization(session, TABLE,
                Collections.singletonList("null_col")));
    }

    private void readInThisStatement(Schema schema, LanceIndexMetadata indexes) {
        LanceTableMetadata metadata = new LanceTableMetadata(new LanceTableAccess(URI,
                Collections.singletonMap("aws_region", "us-east-1"), false), 3, schema,
                Arrays.asList(new LanceFragmentInfo(1, 8, 8), new LanceFragmentInfo(2, 7, 7)));
        LanceStatementScope.sharedMetadata(session, TABLE, () -> metadata);
        if (indexes != null) {
            LanceStatementScope.sharedIndexMetadata(session, TABLE.withVersion(3), metadata, () -> indexes);
        }
    }

    private static LanceIndexMetadata indexes(Map<String, Integer> fieldIds) {
        return new LanceIndexMetadata(fieldIds, Collections.singletonList(new LanceIndexSegmentInfo(SEGMENT,
                "vector_idx", Collections.singletonList(9), Collections.singletonList(1L), IndexType.IVF_PQ, "L2")));
    }

    private static LanceConnectorMetadata metadata() {
        return new LanceConnectorMetadata(unopenableClient());
    }

    private static LanceScanPlanProvider provider() {
        return new LanceScanPlanProvider(unopenableClient(), "filesystem", () -> Collections.singletonMap("k", "v"));
    }

    private static LanceCatalogClient unopenableClient() {
        Map<String, String> properties = new HashMap<>();
        properties.put("type", "lance");
        properties.put("warehouse", "s3://warehouse/lance");
        return new LanceCatalogClient(LanceCatalogProperties.of(properties),
                () -> {
                    throw new AssertionError("the statement already read this version; nothing may read it again");
                },
                () -> {
                    throw new AssertionError("the statement already read this version; nothing may read it again");
                });
    }

    private static ConnectorSearchRequest vector(Map<String, String> arguments) {
        return new ConnectorSearchRequest(ConnectorSearchFunction.VECTOR_SEARCH, arguments, Collections.emptySet());
    }

    private static ConnectorSearchRequest fullText(Map<String, String> arguments) {
        return new ConnectorSearchRequest(ConnectorSearchFunction.FULL_TEXT_SEARCH, arguments,
                Collections.emptySet());
    }

    private static Map<String, String> map(String... keysAndValues) {
        Map<String, String> map = new HashMap<>();
        for (int i = 0; i < keysAndValues.length; i += 2) {
            map.put(keysAndValues[i], keysAndValues[i + 1]);
        }
        return map;
    }
}
