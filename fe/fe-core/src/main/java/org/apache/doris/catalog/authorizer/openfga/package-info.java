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

/**
 * An OpenFGA backed {@code CatalogAccessController} for Doris.
 *
 * <p>This package implements the {@code AccessControllerFactory} / {@code CatalogAccessController}
 * SPI with OpenFGA (an open source Zanzibar implementation) as the external authorization backend,
 * as an alternative to the Apache Ranger controller in
 * {@code org.apache.doris.catalog.authorizer.ranger}. Doris object privilege checks are translated
 * into OpenFGA relationship Check calls over a hierarchical authorization model. The model
 * ({@code schema.fga}) and configuration are documented under
 * {@code fe/fe-core/src/main/resources/authorizer/openfga/}.
 */
package org.apache.doris.catalog.authorizer.openfga;
