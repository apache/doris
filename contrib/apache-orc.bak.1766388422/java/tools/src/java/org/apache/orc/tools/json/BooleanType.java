/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.orc.tools.json;

import org.apache.orc.TypeDescription;

/**
 * A type that represents true, false, and null.
 */
class BooleanType extends HiveType {
  BooleanType() {
    super(Kind.BOOLEAN);
  }

  @Override
  public String toString() {
    return "boolean";
  }

  @Override
  public boolean subsumes(HiveType other) {
    return other.kind == Kind.BOOLEAN || other.kind == Kind.NULL;
  }

  @Override
  public void merge(HiveType other) {
    // nothing to do to merge boolean types
  }

  @Override
  public TypeDescription getSchema() {
    return TypeDescription.createBoolean();
  }
}
