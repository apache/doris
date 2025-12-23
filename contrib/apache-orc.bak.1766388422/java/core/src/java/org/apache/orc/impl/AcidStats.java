/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 * <p/>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p/>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.orc.impl;

/**
 * Statistics about the ACID operations in an ORC file
 */
public class AcidStats {
  public long inserts;
  public long updates;
  public long deletes;

  public AcidStats() {
    inserts = 0;
    updates = 0;
    deletes = 0;
  }

  public AcidStats(String serialized) {
    String[] parts = serialized.split(",");
    inserts = Long.parseLong(parts[0]);
    updates = Long.parseLong(parts[1]);
    deletes = Long.parseLong(parts[2]);
  }

  public String serialize() {
    StringBuilder builder = new StringBuilder();
    builder.append(inserts);
    builder.append(",");
    builder.append(updates);
    builder.append(",");
    builder.append(deletes);
    return builder.toString();
  }

  @Override
  public String toString() {
    StringBuilder builder = new StringBuilder();
    builder.append(" inserts: ").append(inserts);
    builder.append(" updates: ").append(updates);
    builder.append(" deletes: ").append(deletes);
    return builder.toString();
  }
}
