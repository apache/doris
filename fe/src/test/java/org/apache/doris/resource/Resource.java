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

package org.apache.doris.resource;

/*
 * Resource is the collective name of the nodes that provide various service capabilities in Doris cluster.
 * Each resource has a unique ID.
 * A resource may have one or more tags that represent the functional properties or custom groupings of a resource, etc.
 * eg:
 *      Backend, Frontend, Broker, RemoteStorage
 */
public abstract class Resource {
    private long id;
    private TagSet tagSet;
}
