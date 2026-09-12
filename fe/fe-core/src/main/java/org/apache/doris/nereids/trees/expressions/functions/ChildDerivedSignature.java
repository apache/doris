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

package org.apache.doris.nereids.trees.expressions.functions;

import org.apache.doris.catalog.FunctionSignature;

/**
 * A function whose signature embeds type metadata derived directly from its children.
 *
 * <p>This hook runs only after the framework has reused an already-resolved signature. Implementations rebuild the
 * same function shape from the current children; they must not search overloads or rerun generic signature
 * computation. This keeps binding, coercion, and precision decisions frozen while allowing nested complex-type
 * metadata to follow equivalent child rewrites.</p>
 */
public interface ChildDerivedSignature extends ComputeSignature {

    /** Derive the signature metadata owned by this function from its current children. */
    FunctionSignature deriveSignatureFromChildren(FunctionSignature resolvedSignature);

    @Override
    default FunctionSignature refreshDerivedSignature(FunctionSignature resolvedSignature) {
        FunctionSignature refreshed = deriveSignatureFromChildren(resolvedSignature);
        return resolvedSignature.equals(refreshed) ? resolvedSignature : refreshed;
    }
}
