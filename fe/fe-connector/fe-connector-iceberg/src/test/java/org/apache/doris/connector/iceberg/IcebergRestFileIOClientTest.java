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

package org.apache.doris.connector.iceberg;

import org.apache.iceberg.PartitionSpec;
import org.apache.iceberg.Schema;
import org.apache.iceberg.TableMetadata;
import org.apache.iceberg.exceptions.CleanableFailure;
import org.apache.iceberg.exceptions.NotAuthorizedException;
import org.apache.iceberg.rest.HTTPRequest;
import org.apache.iceberg.rest.ParserContext;
import org.apache.iceberg.rest.RESTClient;
import org.apache.iceberg.rest.RESTRequest;
import org.apache.iceberg.rest.RESTResponse;
import org.apache.iceberg.rest.auth.AuthSession;
import org.apache.iceberg.rest.requests.CreateTableRequest;
import org.apache.iceberg.rest.requests.ImmutableRegisterTableRequest;
import org.apache.iceberg.rest.requests.UpdateTableRequest;
import org.apache.iceberg.rest.responses.ConfigResponse;
import org.apache.iceberg.rest.responses.ErrorResponse;
import org.apache.iceberg.rest.responses.LoadTableResponse;
import org.apache.iceberg.types.Types;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.EnumSource;
import org.junit.jupiter.params.provider.MethodSource;
import org.junit.jupiter.params.provider.ValueSource;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Consumer;
import java.util.function.Supplier;

/** Exercises the external RESTClient boundary without starting a server or replacing Doris internals. */
class IcebergRestFileIOClientTest {

    @Test
    void getWithParserContextPreservesRequestAndAdaptsSuccessfulResponseOnce() {
        ConfigResponse original = ConfigResponse.builder().withDefault("fixture", "original").build();
        ConfigResponse adapted = ConfigResponse.builder().withDefault("fixture", "adapted").build();
        RecordingRESTClient delegate = new RecordingRESTClient(original);
        AtomicInteger adapterCalls = new AtomicInteger();
        RESTClient client = new IcebergRestFileIOClient(delegate, response -> {
            Assertions.assertSame(original, response);
            adapterCalls.incrementAndGet();
            return adapted;
        });
        String path = "v1/config";
        Map<String, String> query = Map.of("warehouse", "fixture-warehouse");
        Map<String, String> headers = Map.of("X-Test-Header", "header-value");
        Consumer<ErrorResponse> errorHandler = error -> Assertions.fail("Unexpected REST error");
        ParserContext parserContext = ParserContext.builder().add("fixture-context", "value").build();

        ConfigResponse response = client.get(path, query, ConfigResponse.class, headers,
                errorHandler, parserContext);

        Assertions.assertSame(adapted, response);
        Assertions.assertEquals(1, adapterCalls.get());
        Assertions.assertEquals(1, delegate.getCalls);
        Assertions.assertEquals(path, delegate.path);
        Assertions.assertEquals(query, delegate.query);
        Assertions.assertSame(ConfigResponse.class, delegate.responseType);
        Assertions.assertEquals(headers, delegate.headers);
        Assertions.assertSame(errorHandler, delegate.errorHandler);
        Assertions.assertSame(parserContext, delegate.parserContext);
    }

    @Test
    void getEvaluatesSuppliedHeadersOncePerRequestWithoutCachingThem() {
        ConfigResponse original = ConfigResponse.builder().build();
        ConfigResponse adapted = ConfigResponse.builder().withDefault("fixture", "adapted").build();
        RecordingRESTClient delegate = new RecordingRESTClient(original);
        AtomicInteger adapterCalls = new AtomicInteger();
        RESTClient client = new IcebergRestFileIOClient(delegate, response -> {
            adapterCalls.incrementAndGet();
            return adapted;
        });
        AtomicInteger headerCalls = new AtomicInteger();
        Supplier<Map<String, String>> headers = () -> Map.of("X-Generation",
                Integer.toString(headerCalls.incrementAndGet()));

        Assertions.assertSame(adapted, client.get("v1/config", ConfigResponse.class, headers, null));
        Assertions.assertEquals(1, headerCalls.get());
        Assertions.assertEquals(Map.of("X-Generation", "1"), delegate.headers);
        Assertions.assertTrue(delegate.query.isEmpty());

        Map<String, String> query = Map.of("warehouse", "fixture");
        Assertions.assertSame(adapted, client.get("v1/config", query, ConfigResponse.class, headers, null));
        Assertions.assertEquals(2, headerCalls.get());
        Assertions.assertEquals(Map.of("X-Generation", "2"), delegate.headers);
        Assertions.assertEquals(query, delegate.query);
        Assertions.assertEquals(2, adapterCalls.get());
        Assertions.assertEquals(2, delegate.getCalls);
    }

    @ParameterizedTest
    @EnumSource(PostVariant.class)
    void successfulCommitIsReturnedWithoutAdaptingStorageConfiguration(PostVariant variant) {
        TableMetadata metadata = TableMetadata.newTableMetadata(
                new Schema(Types.NestedField.required(1, "id", Types.LongType.get())),
                PartitionSpec.unpartitioned(), "abfs://container@account.dfs.core.windows.net/table", Map.of());
        LoadTableResponse committed = LoadTableResponse.builder().withTableMetadata(metadata)
                .addConfig("adls.sas-token.account.dfs.core.windows.net", "sig=fixture&se=2000-01-01T00:00:00Z")
                .build();
        RecordingRESTClient delegate = new RecordingRESTClient(committed);
        RESTClient client = rejectingBothAdapters(delegate);
        RESTRequest request = new UpdateTableRequest(List.of(), List.of());
        String path = "v1/namespaces/ns/tables/table";
        Map<String, String> headers = Map.of("X-Test-Header", "fixture");
        AtomicInteger headerCalls = new AtomicInteger();
        Supplier<Map<String, String>> headerSupplier = () -> {
            headerCalls.incrementAndGet();
            return headers;
        };
        Consumer<ErrorResponse> errorHandler = error -> Assertions.fail("Unexpected commit failure");
        List<Map<String, String>> receivedHeaders = new ArrayList<>();
        Consumer<Map<String, String>> callback = receivedHeaders::add;
        ParserContext parserContext = ParserContext.builder().add("fixture-context", "value").build();

        LoadTableResponse actual = switch (variant) {
            case BASIC -> client.post(path, request, LoadTableResponse.class, headers, errorHandler);
            case CALLBACK -> client.post(path, request, LoadTableResponse.class, headers, errorHandler, callback);
            case CONTEXT -> client.post(path, request, LoadTableResponse.class, headers,
                    errorHandler, callback, parserContext);
            case SUPPLIER -> client.post(path, request, LoadTableResponse.class, headerSupplier, errorHandler);
            case SUPPLIER_CALLBACK -> client.post(path, request, LoadTableResponse.class, headerSupplier,
                    errorHandler, callback);
        };

        Assertions.assertSame(committed, actual);
        Assertions.assertEquals(1, delegate.postCalls);
        Assertions.assertSame(request, delegate.body);
        Assertions.assertEquals(path, delegate.path);
        Assertions.assertSame(LoadTableResponse.class, delegate.responseType);
        Assertions.assertEquals(headers, delegate.headers);
        Assertions.assertSame(errorHandler, delegate.errorHandler);
        Assertions.assertEquals(variant.suppliedHeaders ? 1 : 0, headerCalls.get());
        Assertions.assertEquals(variant.callback ? List.of(delegate.sentResponseHeaders) : List.of(), receivedHeaders);
        Assertions.assertSame(variant.callback ? callback : null, delegate.responseHeaders);
        Assertions.assertSame(variant == PostVariant.CONTEXT ? parserContext : null, delegate.parserContext);
    }

    @Test
    void createResponseReceivesFileIOCredentialsWithoutUsingTheGetAdapter() {
        Schema schema = new Schema(Types.NestedField.required(1, "id", Types.LongType.get()));
        TableMetadata metadata = TableMetadata.newTableMetadata(schema, PartitionSpec.unpartitioned(),
                "abfs://container@account.dfs.core.windows.net/table", Map.of());
        LoadTableResponse original = LoadTableResponse.builder().withTableMetadata(metadata).build();
        LoadTableResponse adapted = LoadTableResponse.builder().withTableMetadata(metadata)
                .addConfig("adls.sas-token.account.dfs.core.windows.net", "sig=creation-fixture").build();
        RecordingRESTClient delegate = new RecordingRESTClient(original);
        RESTClient client = new IcebergRestFileIOClient(delegate, response -> {
            throw new AssertionError("Create must not use the GET adapter");
        }, response -> {
            Assertions.assertSame(original, response);
            return adapted;
        });
        CreateTableRequest request = CreateTableRequest.builder().withName("table").withSchema(schema).build();

        Assertions.assertSame(adapted, client.post("v1/namespaces/ns/tables", request,
                LoadTableResponse.class, Map.of(), null));
        Assertions.assertEquals(1, delegate.postCalls);
        Assertions.assertSame(request, delegate.body);
    }

    @ParameterizedTest
    @MethodSource("creationPostVariants")
    void initializationPostAdaptsOnceAndPreservesRequestContract(CreationPhase phase, PostVariant variant) {
        LoadTableResponse original = tableResponse();
        LoadTableResponse adapted = LoadTableResponse.builder().withTableMetadata(original.tableMetadata())
                .addConfig("adls.sas-token.account.dfs.core.windows.net", "sig=creation-fixture").build();
        RecordingRESTClient delegate = new RecordingRESTClient(original);
        AtomicInteger adapterCalls = new AtomicInteger();
        List<String> events = new ArrayList<>();
        RESTClient client = new IcebergRestFileIOClient(delegate, response -> {
            throw new AssertionError("Initialization POST must not use the GET adapter");
        }, response -> {
            Assertions.assertSame(original, response);
            Assertions.assertEquals(1, delegate.postCalls);
            adapterCalls.incrementAndGet();
            events.add("adapt");
            return adapted;
        });
        RESTRequest request = phase.request();
        String path = "v1/namespaces/ns/tables";
        Map<String, String> headers = Map.of("X-Test-Header", "fixture");
        AtomicInteger headerCalls = new AtomicInteger();
        Supplier<Map<String, String>> headerSupplier = () -> {
            headerCalls.incrementAndGet();
            return headers;
        };
        Consumer<ErrorResponse> errorHandler = error -> Assertions.fail("Unexpected REST failure");
        Consumer<Map<String, String>> callback = responseHeaders -> {
            Assertions.assertEquals(delegate.sentResponseHeaders, responseHeaders);
            events.add("response-headers");
        };
        ParserContext parserContext = ParserContext.builder().add("fixture-context", "value").build();

        LoadTableResponse actual = switch (variant) {
            case BASIC -> client.post(path, request, LoadTableResponse.class, headers, errorHandler);
            case CALLBACK -> client.post(path, request, LoadTableResponse.class, headers, errorHandler, callback);
            case CONTEXT -> client.post(path, request, LoadTableResponse.class, headers,
                    errorHandler, callback, parserContext);
            case SUPPLIER -> client.post(path, request, LoadTableResponse.class, headerSupplier, errorHandler);
            case SUPPLIER_CALLBACK -> client.post(path, request, LoadTableResponse.class, headerSupplier,
                    errorHandler, callback);
        };

        Assertions.assertSame(adapted, actual);
        Assertions.assertEquals(1, adapterCalls.get());
        Assertions.assertEquals(1, delegate.postCalls);
        Assertions.assertSame(request, delegate.body);
        Assertions.assertEquals(path, delegate.path);
        Assertions.assertSame(LoadTableResponse.class, delegate.responseType);
        Assertions.assertEquals(headers, delegate.headers);
        Assertions.assertSame(errorHandler, delegate.errorHandler);
        Assertions.assertEquals(variant.suppliedHeaders ? 1 : 0, headerCalls.get());
        Assertions.assertSame(variant.callback ? callback : null, delegate.responseHeaders);
        Assertions.assertSame(variant == PostVariant.CONTEXT ? parserContext : null, delegate.parserContext);
        Assertions.assertEquals(variant.callback ? List.of("response-headers", "adapt") : List.of("adapt"), events);
    }

    @ParameterizedTest
    @EnumSource(CreationPhase.class)
    void twoArgumentClientLeavesInitializationPostUnchanged(CreationPhase phase) {
        LoadTableResponse original = tableResponse();
        RecordingRESTClient delegate = new RecordingRESTClient(original);

        Assertions.assertSame(original, rejectingAdapter(delegate).post("v1/table", phase.request(),
                LoadTableResponse.class, Map.of(), null));
        Assertions.assertEquals(1, delegate.postCalls);
    }

    @ParameterizedTest
    @EnumSource(CreationPhase.class)
    void initializationFailureReportsCompletedPhaseAndPreservesCause(CreationPhase phase) {
        RecordingRESTClient delegate = new RecordingRESTClient(tableResponse());
        RuntimeException cause = new IllegalArgumentException("fixture-secret-sentinel", new NotAuthorizedException(
                "fixture-secret-sentinel"));
        RESTClient client = new IcebergRestFileIOClient(delegate, response -> response, response -> {
            throw cause;
        });

        IcebergPostSuccessFileIOInitializationException failure = Assertions.assertThrows(
                IcebergPostSuccessFileIOInitializationException.class,
                () -> client.post("v1/table", phase.request(), LoadTableResponse.class, Map.of(), null));

        Assertions.assertSame(cause, failure.getCause());
        Assertions.assertTrue(failure.getMessage().contains(phase.operation + " request succeeded"));
        Assertions.assertTrue(failure.getMessage().contains("local FileIO initialization failed"));
        Assertions.assertTrue(failure.getMessage().contains("not retry"));
        Assertions.assertFalse(failure.getMessage().contains("fixture-secret-sentinel"));
        Assertions.assertFalse(CleanableFailure.class.isInstance(failure));
        Assertions.assertEquals(phase == CreationPhase.STAGE_CREATE,
                failure.getMessage().contains("transaction has not been committed"));
        Assertions.assertSame(failure, IcebergPostSuccessFileIOInitializationException.find(
                new RuntimeException("outer catalog failure", failure)).orElseThrow());
        Assertions.assertEquals(1, delegate.postCalls);
    }

    @ParameterizedTest
    @EnumSource(CreationPhase.class)
    void rejectedInitializationPostIsNotMisreportedAsSuccessful(CreationPhase phase) {
        RecordingRESTClient delegate = new RecordingRESTClient(null);
        delegate.errorResponse = ErrorResponse.builder().responseCode(401)
                .withType("NotAuthorizedException").withMessage("Fixture authorization failure").build();
        NotAuthorizedException cause = new NotAuthorizedException("Fixture authorization failure");
        List<String> events = new ArrayList<>();
        Consumer<ErrorResponse> errorHandler = response -> {
            Assertions.assertSame(delegate.errorResponse, response);
            events.add("error-handler");
            throw cause;
        };
        Consumer<Map<String, String>> callback = headers -> events.add("response-headers");

        Assertions.assertSame(cause, Assertions.assertThrows(NotAuthorizedException.class,
                () -> rejectingBothAdapters(delegate).post("v1/table", phase.request(), LoadTableResponse.class,
                        Map.of(), errorHandler, callback)));
        Assertions.assertEquals(List.of("response-headers", "error-handler"), events);
        Assertions.assertTrue(IcebergPostSuccessFileIOInitializationException.find(cause).isEmpty());
        Assertions.assertEquals(1, delegate.postCalls);
    }

    @Test
    void creationAdapterRequiresALoadTableResponse() {
        ConfigResponse original = ConfigResponse.builder().build();
        RecordingRESTClient otherResponseDelegate = new RecordingRESTClient(original);
        RecordingRESTClient emptyResponseDelegate = new RecordingRESTClient(null);
        RESTRequest request = CreationPhase.CREATE.request();

        Assertions.assertSame(original, rejectingBothAdapters(otherResponseDelegate).post("v1/table", request,
                ConfigResponse.class, Map.of(), null));
        Assertions.assertNull(rejectingBothAdapters(emptyResponseDelegate).post("v1/table", request,
                LoadTableResponse.class, Map.of(), null));
        Assertions.assertEquals(1, otherResponseDelegate.postCalls);
        Assertions.assertEquals(1, emptyResponseDelegate.postCalls);
    }

    @Test
    void sessionRebindingRetainsInitializationAdapter() {
        LoadTableResponse original = tableResponse();
        LoadTableResponse adapted = LoadTableResponse.builder().withTableMetadata(original.tableMetadata())
                .addConfig("adls.sas-token.account.dfs.core.windows.net", "sig=creation-fixture").build();
        RecordingRESTClient delegate = new RecordingRESTClient(original);
        AtomicInteger adapterCalls = new AtomicInteger();
        RESTClient root = new IcebergRestFileIOClient(delegate, response -> {
            throw new AssertionError("Initialization POST must not use the GET adapter");
        }, response -> {
            Assertions.assertSame(original, response);
            adapterCalls.incrementAndGet();
            return adapted;
        });
        RecordingAuthSession session = new RecordingAuthSession();
        RESTClient child = root.withAuthSession(session);

        Assertions.assertSame(adapted, child.post("v1/table", CreationPhase.REGISTER.request(),
                LoadTableResponse.class, Map.of(), null));
        Assertions.assertSame(session, delegate.authenticatedClient.session);
        Assertions.assertEquals(0, delegate.postCalls);
        Assertions.assertEquals(1, delegate.authenticatedClient.postCalls);
        Assertions.assertEquals(1, adapterCalls.get());
    }

    @Test
    void queryDeletePreservesParametersAndEvaluatesSuppliedHeadersOnce() {
        ConfigResponse response = ConfigResponse.builder().build();
        RecordingRESTClient delegate = new RecordingRESTClient(response);
        RESTClient client = rejectingAdapter(delegate);
        Map<String, String> query = Map.of("purgeRequested", "true");
        Map<String, String> headers = Map.of("X-Test-Header", "fixture");
        AtomicInteger headerCalls = new AtomicInteger();
        Consumer<ErrorResponse> errorHandler = error -> Assertions.fail("Unexpected delete failure");

        Assertions.assertSame(response, client.delete("v1/table", query, ConfigResponse.class, () -> {
            headerCalls.incrementAndGet();
            return headers;
        }, errorHandler));

        Assertions.assertEquals(1, delegate.deleteCalls);
        Assertions.assertEquals(1, headerCalls.get());
        Assertions.assertEquals("v1/table", delegate.path);
        Assertions.assertEquals(query, delegate.query);
        Assertions.assertEquals(headers, delegate.headers);
        Assertions.assertSame(errorHandler, delegate.errorHandler);
    }

    @Test
    void deleteWithoutQueryReturnsTheOriginalResponse() {
        ConfigResponse response = ConfigResponse.builder().build();
        RecordingRESTClient delegate = new RecordingRESTClient(response);

        Assertions.assertSame(response, rejectingAdapter(delegate).delete("v1/table", ConfigResponse.class,
                Map.of("X-Test-Header", "fixture"), null));

        Assertions.assertEquals(1, delegate.deleteCalls);
        Assertions.assertTrue(delegate.query.isEmpty());
        Assertions.assertEquals(Map.of("X-Test-Header", "fixture"), delegate.headers);
    }

    @Test
    void headPreservesSuppliedHeadersWithoutAdaptingAResponse() {
        RecordingRESTClient delegate = new RecordingRESTClient(null);
        AtomicInteger headerCalls = new AtomicInteger();
        Map<String, String> headers = Map.of("X-Test-Header", "fixture");
        Consumer<ErrorResponse> errorHandler = error -> Assertions.fail("Unexpected HEAD failure");

        rejectingAdapter(delegate).head("v1/table", () -> {
            headerCalls.incrementAndGet();
            return headers;
        }, errorHandler);

        Assertions.assertEquals(1, delegate.headCalls);
        Assertions.assertEquals(1, headerCalls.get());
        Assertions.assertEquals("v1/table", delegate.path);
        Assertions.assertEquals(headers, delegate.headers);
        Assertions.assertSame(errorHandler, delegate.errorHandler);
    }

    @Test
    void formPostPreservesPayloadAndSuppliedHeadersWithoutAdapting() {
        ConfigResponse response = ConfigResponse.builder().build();
        RecordingRESTClient delegate = new RecordingRESTClient(response);
        Map<String, String> form = Map.of("grant_type", "client_credentials");
        Map<String, String> headers = Map.of("X-Test-Header", "fixture");
        AtomicInteger headerCalls = new AtomicInteger();
        Consumer<ErrorResponse> errorHandler = error -> Assertions.fail("Unexpected form failure");

        Assertions.assertSame(response, rejectingAdapter(delegate).postForm("v1/oauth/tokens", form,
                ConfigResponse.class, () -> {
                    headerCalls.incrementAndGet();
                    return headers;
                }, errorHandler));

        Assertions.assertEquals(1, delegate.formCalls);
        Assertions.assertEquals(1, headerCalls.get());
        Assertions.assertEquals("v1/oauth/tokens", delegate.path);
        Assertions.assertEquals(form, delegate.formData);
        Assertions.assertEquals(headers, delegate.headers);
        Assertions.assertSame(errorHandler, delegate.errorHandler);
    }

    @Test
    void sessionRebindingKeepsAdapterAndClosesOnlyTheSelectedClient() throws IOException {
        ConfigResponse original = ConfigResponse.builder().build();
        ConfigResponse adapted = ConfigResponse.builder().withDefault("fixture", "adapted").build();
        RecordingRESTClient delegate = new RecordingRESTClient(original);
        AtomicInteger adapterCalls = new AtomicInteger();
        RESTClient root = new IcebergRestFileIOClient(delegate, response -> {
            Assertions.assertSame(original, response);
            adapterCalls.incrementAndGet();
            return adapted;
        });
        RecordingAuthSession firstSession = new RecordingAuthSession();
        RecordingAuthSession secondSession = new RecordingAuthSession();
        RESTClient first = root.withAuthSession(firstSession);
        RecordingRESTClient firstDelegate = delegate.authenticatedClient;
        RESTClient second = first.withAuthSession(secondSession);
        RecordingRESTClient secondDelegate = firstDelegate.authenticatedClient;

        Assertions.assertSame(adapted, second.get("v1/config", ConfigResponse.class, Map.of(), null));
        Assertions.assertSame(firstSession, firstDelegate.session);
        Assertions.assertSame(secondSession, secondDelegate.session);
        Assertions.assertEquals(1, adapterCalls.get());
        Assertions.assertEquals(0, delegate.getCalls);
        Assertions.assertEquals(0, firstDelegate.getCalls);
        Assertions.assertEquals(1, secondDelegate.getCalls);

        first.close();
        Assertions.assertEquals(1, firstDelegate.closeCalls);
        Assertions.assertEquals(0, delegate.closeCalls);
        Assertions.assertEquals(0, secondDelegate.closeCalls);
        second.close();
        root.close();
        Assertions.assertEquals(1, secondDelegate.closeCalls);
        Assertions.assertEquals(1, delegate.closeCalls);
        Assertions.assertEquals(0, firstSession.closeCalls);
        Assertions.assertEquals(0, secondSession.closeCalls);
    }

    @ParameterizedTest
    @ValueSource(booleans = {true, false})
    void emptyGetResponseDoesNotInvokeAdapter(boolean hasResponseType) {
        RecordingRESTClient delegate = new RecordingRESTClient(null);
        Class<ConfigResponse> responseType = hasResponseType ? ConfigResponse.class : null;

        Assertions.assertNull(rejectingAdapter(delegate).get("v1/config", Map.of(), responseType, Map.of(), null));
        Assertions.assertEquals(1, delegate.getCalls);
    }

    @Test
    void getFailurePropagatesWithoutAdaptingOrRetrying() {
        RecordingRESTClient delegate = new RecordingRESTClient(null);
        NotAuthorizedException failure = new NotAuthorizedException("Fixture authorization failure");
        delegate.requestFailure = failure;

        Assertions.assertSame(failure, Assertions.assertThrows(NotAuthorizedException.class,
                () -> rejectingAdapter(delegate).get("v1/config", ConfigResponse.class, Map.of(), null)));
        Assertions.assertEquals(1, delegate.getCalls);
    }

    @Test
    void adapterFailurePropagatesWithoutAnotherRequest() {
        RecordingRESTClient delegate = new RecordingRESTClient(ConfigResponse.builder().build());
        IllegalArgumentException failure = new IllegalArgumentException("Fixture metadata configuration failure");
        RESTClient client = new IcebergRestFileIOClient(delegate, response -> {
            throw failure;
        });

        Assertions.assertSame(failure, Assertions.assertThrows(IllegalArgumentException.class,
                () -> client.get("v1/config", ConfigResponse.class, Map.of(), null)));
        Assertions.assertEquals(1, delegate.getCalls);
    }

    @Test
    void rejectedPostPreservesResponseHeaderCallbackAndErrorHandlerOrder() {
        RecordingRESTClient delegate = new RecordingRESTClient(null);
        delegate.errorResponse = ErrorResponse.builder().responseCode(401)
                .withType("NotAuthorizedException").withMessage("Fixture authorization failure").build();
        NotAuthorizedException failure = new NotAuthorizedException("Fixture authorization failure");
        List<String> events = new ArrayList<>();
        Consumer<Map<String, String>> callback = headers -> {
            Assertions.assertEquals(delegate.sentResponseHeaders, headers);
            events.add("response-headers");
        };
        Consumer<ErrorResponse> errorHandler = error -> {
            Assertions.assertSame(delegate.errorResponse, error);
            events.add("error-handler");
            throw failure;
        };
        RESTRequest request = new UpdateTableRequest(List.of(), List.of());

        Assertions.assertSame(failure, Assertions.assertThrows(NotAuthorizedException.class,
                () -> rejectingAdapter(delegate).post("v1/table", request, LoadTableResponse.class,
                        Map.of(), errorHandler, callback)));
        Assertions.assertEquals(List.of("response-headers", "error-handler"), events);
        Assertions.assertEquals(1, delegate.postCalls);
    }

    @Test
    void closeFailurePropagatesUnchanged() {
        RecordingRESTClient delegate = new RecordingRESTClient(null);
        IOException failure = new IOException("Fixture close failure");
        delegate.closeFailure = failure;

        Assertions.assertSame(failure, Assertions.assertThrows(IOException.class,
                () -> rejectingAdapter(delegate).close()));
        Assertions.assertEquals(1, delegate.closeCalls);
    }

    private static RESTClient rejectingAdapter(RESTClient delegate) {
        return new IcebergRestFileIOClient(delegate, response -> {
            throw new AssertionError("This response must not be adapted");
        });
    }

    private static RESTClient rejectingBothAdapters(RESTClient delegate) {
        return new IcebergRestFileIOClient(delegate, response -> {
            throw new AssertionError("This response must not use the GET adapter");
        }, response -> {
            throw new AssertionError("This response must not use the initialization adapter");
        });
    }

    private static LoadTableResponse tableResponse() {
        TableMetadata metadata = TableMetadata.newTableMetadata(
                new Schema(Types.NestedField.required(1, "id", Types.LongType.get())),
                PartitionSpec.unpartitioned(), "abfs://container@account.dfs.core.windows.net/table", Map.of());
        return LoadTableResponse.builder().withTableMetadata(metadata).build();
    }

    private static List<Arguments> creationPostVariants() {
        List<Arguments> variants = new ArrayList<>();
        for (CreationPhase phase : CreationPhase.values()) {
            for (PostVariant variant : PostVariant.values()) {
                variants.add(Arguments.of(phase, variant));
            }
        }
        return variants;
    }

    private enum CreationPhase {
        CREATE("create-table"),
        STAGE_CREATE("stage-create"),
        REGISTER("register-table");

        private final String operation;

        CreationPhase(String operation) {
            this.operation = operation;
        }

        private RESTRequest request() {
            if (this == REGISTER) {
                return ImmutableRegisterTableRequest.builder().name("table")
                        .metadataLocation("abfs://container@account.dfs.core.windows.net/table/metadata.json").build();
            }
            CreateTableRequest.Builder builder = CreateTableRequest.builder().withName("table")
                    .withSchema(new Schema(Types.NestedField.required(1, "id", Types.LongType.get())));
            if (this == STAGE_CREATE) {
                builder.stageCreate();
            }
            return builder.build();
        }
    }

    private enum PostVariant {
        BASIC(false, false),
        CALLBACK(true, false),
        CONTEXT(true, false),
        SUPPLIER(false, true),
        SUPPLIER_CALLBACK(true, true);

        private final boolean callback;
        private final boolean suppliedHeaders;

        PostVariant(boolean callback, boolean suppliedHeaders) {
            this.callback = callback;
            this.suppliedHeaders = suppliedHeaders;
        }
    }

    private static final class RecordingAuthSession implements AuthSession {
        private int closeCalls;

        @Override
        public HTTPRequest authenticate(HTTPRequest request) {
            return request;
        }

        @Override
        public void close() {
            closeCalls++;
        }
    }

    private static final class RecordingRESTClient implements RESTClient {
        private final RESTResponse response;
        private final Map<String, String> sentResponseHeaders = Map.of("X-Request-Id", "fixture-request");
        private int getCalls;
        private int postCalls;
        private int deleteCalls;
        private int headCalls;
        private int formCalls;
        private int closeCalls;
        private String path;
        private Map<String, String> query;
        private Class<?> responseType;
        private Map<String, String> headers;
        private Consumer<ErrorResponse> errorHandler;
        private Consumer<Map<String, String>> responseHeaders;
        private ParserContext parserContext;
        private RESTRequest body;
        private Map<String, String> formData;
        private AuthSession session;
        private RecordingRESTClient authenticatedClient;
        private RuntimeException requestFailure;
        private ErrorResponse errorResponse;
        private IOException closeFailure;

        private RecordingRESTClient(RESTResponse response) {
            this.response = response;
        }

        @Override
        public <T extends RESTResponse> T get(String path, Map<String, String> queryParams,
                Class<T> responseType, Map<String, String> headers, Consumer<ErrorResponse> errorHandler,
                ParserContext parserContext) {
            getCalls++;
            record(path, queryParams, responseType, headers, errorHandler);
            this.parserContext = parserContext;
            return finish(responseType);
        }

        private void record(String path, Map<String, String> queryParams, Class<?> responseType,
                Map<String, String> headers, Consumer<ErrorResponse> errorHandler) {
            this.path = path;
            this.query = queryParams;
            this.responseType = responseType;
            this.headers = headers;
            this.errorHandler = errorHandler;
        }

        private <T extends RESTResponse> T finish(Class<T> responseType) {
            if (requestFailure != null) {
                throw requestFailure;
            }
            if (errorResponse != null) {
                errorHandler.accept(errorResponse);
                throw new AssertionError("The configured error handler must reject this response");
            }
            return response == null ? null : responseType.cast(response);
        }

        @Override
        public <T extends RESTResponse> T get(String path, Map<String, String> queryParams,
                Class<T> responseType, Map<String, String> headers, Consumer<ErrorResponse> errorHandler) {
            return get(path, queryParams, responseType, headers, errorHandler, null);
        }

        @Override
        public void head(String path, Map<String, String> headers, Consumer<ErrorResponse> errorHandler) {
            headCalls++;
            record(path, Map.of(), null, headers, errorHandler);
            finish(null);
        }

        @Override
        public <T extends RESTResponse> T delete(String path, Class<T> responseType,
                Map<String, String> headers, Consumer<ErrorResponse> errorHandler) {
            return delete(path, Map.of(), responseType, headers, errorHandler);
        }

        @Override
        public <T extends RESTResponse> T delete(String path, Map<String, String> queryParams,
                Class<T> responseType, Map<String, String> headers, Consumer<ErrorResponse> errorHandler) {
            deleteCalls++;
            record(path, queryParams, responseType, headers, errorHandler);
            return finish(responseType);
        }

        @Override
        public <T extends RESTResponse> T post(String path, RESTRequest body, Class<T> responseType,
                Map<String, String> headers, Consumer<ErrorResponse> errorHandler) {
            return post(path, body, responseType, headers, errorHandler, null, null);
        }

        @Override
        public <T extends RESTResponse> T post(String path, RESTRequest body, Class<T> responseType,
                Map<String, String> headers, Consumer<ErrorResponse> errorHandler,
                Consumer<Map<String, String>> responseHeaders) {
            return post(path, body, responseType, headers, errorHandler, responseHeaders, null);
        }

        @Override
        public <T extends RESTResponse> T post(String path, RESTRequest body, Class<T> responseType,
                Map<String, String> headers, Consumer<ErrorResponse> errorHandler,
                Consumer<Map<String, String>> responseHeaders, ParserContext parserContext) {
            postCalls++;
            record(path, Map.of(), responseType, headers, errorHandler);
            this.body = body;
            this.responseHeaders = responseHeaders;
            this.parserContext = parserContext;
            if (responseHeaders != null) {
                responseHeaders.accept(sentResponseHeaders);
            }
            return finish(responseType);
        }

        @Override
        public <T extends RESTResponse> T postForm(String path, Map<String, String> formData,
                Class<T> responseType, Map<String, String> headers, Consumer<ErrorResponse> errorHandler) {
            formCalls++;
            record(path, Map.of(), responseType, headers, errorHandler);
            this.formData = formData;
            return finish(responseType);
        }

        @Override
        public RESTClient withAuthSession(AuthSession session) {
            authenticatedClient = new RecordingRESTClient(response);
            authenticatedClient.session = session;
            return authenticatedClient;
        }

        @Override
        public void close() throws IOException {
            closeCalls++;
            if (closeFailure != null) {
                throw closeFailure;
            }
        }
    }
}
