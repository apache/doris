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

package org.apache.orc.impl;

import org.apache.hadoop.io.Text;

import java.io.IOException;
import java.io.OutputStream;


/**
 * Base implementation for {@link org.apache.orc.impl.Dictionary.VisitorContext} used to traversing
 * all nodes in a dictionary.
 */
public class VisitorContextImpl implements Dictionary.VisitorContext {
  private int originalPosition;
  private int start;
  private int end;
  private final DynamicIntArray keyOffsets;
  private final DynamicByteArray byteArray;
  private final Text text = new Text();

  public VisitorContextImpl(DynamicByteArray byteArray, DynamicIntArray keyOffsets) {
    this.byteArray = byteArray;
    this.keyOffsets = keyOffsets;
  }

  @Override
  public int getOriginalPosition() {
    return originalPosition;
  }

  @Override
  public Text getText() {
    byteArray.setText(text, start, end - start);
    return text;
  }

  @Override
  public void writeBytes(OutputStream out)
      throws IOException {
    byteArray.write(out, start, end - start);
  }

  @Override
  public int getLength() {
    return end - start;
  }

  public void setPosition(int position) {
    originalPosition = position;
    start = keyOffsets.get(originalPosition);
    if (position + 1 == keyOffsets.size()) {
      end = byteArray.size();
    } else {
      end = keyOffsets.get(originalPosition + 1);
    }
  }
}
