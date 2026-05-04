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

package org.apache.doris.connector.api.pushdown;

import org.apache.doris.connector.api.ConnectorType;

import java.util.Collections;
import java.util.List;
import java.util.Objects;

/**
 * A function call expression (catch-all for unsupported function types).
 * Connector plugins can inspect the function name and arguments to decide
 * whether to push down or reject.
 */
public final class ConnectorFunctionCall implements ConnectorExpression {

    private static final long serialVersionUID = 1L;

    private final String functionName;
    private final ConnectorType returnType;
    private final List<ConnectorExpression> arguments;

    public ConnectorFunctionCall(String functionName,
            ConnectorType returnType,
            List<ConnectorExpression> arguments) {
        this.functionName = Objects.requireNonNull(functionName, "functionName");
        this.returnType = Objects.requireNonNull(returnType, "returnType");
        this.arguments = arguments == null
                ? Collections.emptyList()
                : Collections.unmodifiableList(arguments);
    }

    public String getFunctionName() {
        return functionName;
    }

    public ConnectorType getReturnType() {
        return returnType;
    }

    public List<ConnectorExpression> getArguments() {
        return arguments;
    }

    @Override
    public List<ConnectorExpression> getChildren() {
        return arguments;
    }

    @Override
    public String toString() {
        return functionName + "(" + arguments + ")";
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (!(o instanceof ConnectorFunctionCall)) {
            return false;
        }
        ConnectorFunctionCall that = (ConnectorFunctionCall) o;
        return functionName.equals(that.functionName)
                && returnType.equals(that.returnType)
                && arguments.equals(that.arguments);
    }

    @Override
    public int hashCode() {
        return Objects.hash(functionName, returnType, arguments);
    }
}
