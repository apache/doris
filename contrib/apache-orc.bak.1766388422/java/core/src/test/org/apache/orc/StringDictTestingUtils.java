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
package org.apache.orc;

import org.apache.hadoop.io.DataOutputBuffer;
import org.apache.orc.impl.Dictionary;

import java.io.IOException;
import java.nio.charset.StandardCharsets;

import static org.junit.jupiter.api.Assertions.assertEquals;


/**
 * The utility class for testing different implementation of dictionary.
 */
public class StringDictTestingUtils {
  private StringDictTestingUtils() {
    // Avoid accidental instantiate
  }

  public static void checkContents(Dictionary dict, int[] order, String... params)
      throws IOException {
    dict.visit(new MyVisitor(params, order));
  }

  public static class MyVisitor implements Dictionary.Visitor {
    private final String[] words;
    private final int[] order;
    private final DataOutputBuffer buffer = new DataOutputBuffer();
    int current = 0;

    MyVisitor(String[] args, int[] order) {
      words = args;
      this.order = order;
    }

    @Override
    public void visit(Dictionary.VisitorContext context)
        throws IOException {
      String word = context.getText().toString();
      assertEquals(words[current], word, "in word " + current);
      assertEquals(order[current], context.getOriginalPosition(), "in word " + current);
      buffer.reset();
      context.writeBytes(buffer);
      assertEquals(word, new String(buffer.getData(), 0, buffer.getLength(), StandardCharsets.UTF_8));
      current += 1;
    }
  }
}
