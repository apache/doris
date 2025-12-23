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
 * Builds a list of buffer chunks
 */
public class BufferChunkList {
  private BufferChunk head;
  private BufferChunk tail;

  public void add(BufferChunk value) {
    if (head == null) {
      head = value;
      tail = value;
    } else {
      tail.next = value;
      value.prev = tail;
      value.next = null;
      tail = value;
    }
  }

  public BufferChunk get() {
    return head;
  }

  /**
   * Get the nth element of the list
   * @param chunk the element number to get from 0
   * @return the given element number
   */
  public BufferChunk get(int chunk) {
    BufferChunk ptr = head;
    for(int i=0; i < chunk; ++i) {
      ptr = ptr == null ? null : (BufferChunk) ptr.next;
    }
    return ptr;
  }

  public void clear() {
    head = null;
    tail = null;
  }
}
