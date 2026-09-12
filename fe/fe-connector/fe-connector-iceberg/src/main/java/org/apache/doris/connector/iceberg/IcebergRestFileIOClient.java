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

import org.apache.doris.connector.iceberg.IcebergPostSuccessFileIOInitializationException.Operation;

import org.apache.iceberg.rest.ParserContext;
import org.apache.iceberg.rest.RESTClient;
import org.apache.iceberg.rest.RESTRequest;
import org.apache.iceberg.rest.RESTResponse;
import org.apache.iceberg.rest.auth.AuthSession;
import org.apache.iceberg.rest.requests.CreateTableRequest;
import org.apache.iceberg.rest.requests.RegisterTableRequest;
import org.apache.iceberg.rest.responses.ErrorResponse;
import org.apache.iceberg.rest.responses.LoadTableResponse;

import java.io.IOException;
import java.util.Map;
import java.util.Objects;
import java.util.function.Consumer;
import java.util.function.UnaryOperator;

/**
 * Adapts read and table-creation responses before Iceberg constructs its FileIO, retaining SDK REST/auth contracts.
 * Commit responses are untouched; failed local creation adaptation records that the REST request already succeeded.
 */
final class IcebergRestFileIOClient implements RESTClient {
    private final RESTClient delegate;
    private final UnaryOperator<RESTResponse> getResponseAdapter;
    private final UnaryOperator<RESTResponse> creationResponseAdapter;

    IcebergRestFileIOClient(RESTClient delegate, UnaryOperator<RESTResponse> getResponseAdapter) {
        this(delegate, getResponseAdapter, UnaryOperator.identity());
    }

    IcebergRestFileIOClient(RESTClient delegate, UnaryOperator<RESTResponse> getResponseAdapter,
            UnaryOperator<RESTResponse> creationResponseAdapter) {
        this.delegate = Objects.requireNonNull(delegate, "delegate");
        this.getResponseAdapter = Objects.requireNonNull(getResponseAdapter, "getResponseAdapter");
        this.creationResponseAdapter = Objects.requireNonNull(creationResponseAdapter, "creationResponseAdapter");
    }

    @Override
    public void head(String path, Map<String, String> headers, Consumer<ErrorResponse> errorHandler) {
        delegate.head(path, headers, errorHandler);
    }

    @Override
    public <T extends RESTResponse> T delete(String path, Class<T> responseType,
            Map<String, String> headers, Consumer<ErrorResponse> errorHandler) {
        return delegate.delete(path, responseType, headers, errorHandler);
    }

    @Override
    public <T extends RESTResponse> T delete(String path, Map<String, String> queryParams,
            Class<T> responseType, Map<String, String> headers, Consumer<ErrorResponse> errorHandler) {
        return delegate.delete(path, queryParams, responseType, headers, errorHandler);
    }

    @Override
    public <T extends RESTResponse> T get(String path, Map<String, String> queryParams,
            Class<T> responseType, Map<String, String> headers, Consumer<ErrorResponse> errorHandler) {
        return adaptGetResponse(delegate.get(path, queryParams, responseType, headers, errorHandler), responseType);
    }

    @Override
    public <T extends RESTResponse> T get(String path, Map<String, String> queryParams,
            Class<T> responseType, Map<String, String> headers, Consumer<ErrorResponse> errorHandler,
            ParserContext parserContext) {
        return adaptGetResponse(delegate.get(path, queryParams, responseType, headers, errorHandler, parserContext),
                responseType);
    }

    private <T extends RESTResponse> T adaptGetResponse(T response, Class<T> responseType) {
        // HTTPClient returns null for HTTP 204 or requests with no response type. There is no payload to adapt.
        if (response == null) {
            return null;
        }
        return responseType.cast(getResponseAdapter.apply(response));
    }

    @Override
    public <T extends RESTResponse> T post(String path, RESTRequest body, Class<T> responseType,
            Map<String, String> headers, Consumer<ErrorResponse> errorHandler) {
        T response = delegate.post(path, body, responseType, headers, errorHandler);
        return adaptCreationResponse(body, response, responseType);
    }

    @Override
    public <T extends RESTResponse> T post(String path, RESTRequest body, Class<T> responseType,
            Map<String, String> headers, Consumer<ErrorResponse> errorHandler,
            Consumer<Map<String, String>> responseHeaders) {
        T response = delegate.post(path, body, responseType, headers, errorHandler, responseHeaders);
        return adaptCreationResponse(body, response, responseType);
    }

    @Override
    public <T extends RESTResponse> T post(String path, RESTRequest body, Class<T> responseType,
            Map<String, String> headers, Consumer<ErrorResponse> errorHandler,
            Consumer<Map<String, String>> responseHeaders, ParserContext parserContext) {
        T response = delegate.post(path, body, responseType, headers, errorHandler, responseHeaders, parserContext);
        return adaptCreationResponse(body, response, responseType);
    }

    private <T extends RESTResponse> T adaptCreationResponse(RESTRequest body, T response, Class<T> responseType) {
        if (!(response instanceof LoadTableResponse)) {
            return response;
        }
        Operation operation;
        if (body instanceof CreateTableRequest) {
            operation = ((CreateTableRequest) body).stageCreate() ? Operation.STAGE_CREATE : Operation.CREATE;
        } else if (body instanceof RegisterTableRequest) {
            operation = Operation.REGISTER;
        } else {
            // UpdateTableRequest also publishes staged-create transactions. None of its responses rebuild FileIO.
            return response;
        }
        try {
            return responseType.cast(creationResponseAdapter.apply(response));
        } catch (RuntimeException failure) {
            // The delegate already returned successfully. Do not classify this local failure as a retryable REST error.
            throw new IcebergPostSuccessFileIOInitializationException(operation, failure);
        }
    }

    @Override
    public <T extends RESTResponse> T postForm(String path, Map<String, String> formData,
            Class<T> responseType, Map<String, String> headers, Consumer<ErrorResponse> errorHandler) {
        return delegate.postForm(path, formData, responseType, headers, errorHandler);
    }

    @Override
    public RESTClient withAuthSession(AuthSession session) {
        return new IcebergRestFileIOClient(delegate.withAuthSession(session), getResponseAdapter,
                creationResponseAdapter);
    }

    @Override
    public void close() throws IOException {
        // HTTPClient owns root/child connection-pool lifetimes; AuthSession is managed separately by Iceberg.
        delegate.close();
    }
}
