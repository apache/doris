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

public class AndFilter implements VectorFilter {
  public final VectorFilter[] filters;
  private final Selected andBound = new Selected();
  private final Selected andOut = new Selected();

  public AndFilter(VectorFilter[] filters) {
    this.filters = filters;
  }

  @Override
  public void filter(OrcFilterContext fc,
                     Selected bound,
                     Selected selOut) {
    // Each filter restricts the current selection. Make a copy of the current selection that will
    // be used for the next filter and finally output to selOut
    andBound.set(bound);
    andOut.ensureSize(bound.selSize);
    for (VectorFilter f : filters) {
      andOut.clear();
      f.filter(fc, andBound, andOut);
      // Make the current selection the bound for the next filter in AND
      andBound.set(andOut);
    }
    selOut.set(andOut);
  }
}
