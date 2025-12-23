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

package org.apache.orc.impl.filter;

import org.apache.orc.OrcFilterContext;

public class OrFilter implements VectorFilter {

  public final VectorFilter[] filters;
  private final Selected orOut = new Selected();
  private final Selected orBound = new Selected();

  public OrFilter(VectorFilter[] filters) {
    this.filters = filters;
  }

  @Override
  public void filter(OrcFilterContext fc,
                     Selected bound,
                     Selected selOut) {
    orOut.ensureSize(bound.selSize);
    orBound.set(bound);
    for (VectorFilter f : filters) {
      // In case of OR since we have to add to existing output, pass the out as empty
      orOut.clear();
      f.filter(fc, orBound, orOut);
      // During an OR operation the size cannot decrease, merge the current selections into selOut
      selOut.unionDisjoint(orOut);
      // Remove these from the bound as they don't need any further evaluation
      orBound.minus(orOut);
    }
  }
}
