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

package org.apache.doris.datasource.connector.converter;

import org.apache.doris.catalog.VariantType;
import org.apache.doris.thrift.TTypeDesc;

/** Execution-only Variant type used by native external connector readers. */
public final class ConnectorComputeVariantType extends VariantType {

    @Override
    public void toThrift(TTypeDesc container) {
        super.toThrift(container);
        // External native readers always produce ColumnVariantV2, independently of the storage
        // Connector Variant uses the V2 execution carrier.
        container.getTypes().get(container.getTypes().size() - 1).scalar_type.setVariantIsV2(true);
    }
}
