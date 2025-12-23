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

import com.google.gson.JsonArray;
import com.google.gson.JsonElement;
import com.google.gson.JsonObject;
import com.google.gson.JsonPrimitive;
import com.google.gson.JsonStreamParser;

import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.PrintStream;
import java.nio.charset.StandardCharsets;
import java.util.HashMap;
import java.util.Map;
import java.util.zip.GZIPInputStream;

/**
 * This class takes a set of JSON documents and shreds them into a file per
 * a primitive column. This is useful when trying to understand a set of
 * documents by providing sample values for each of the columns.
 *
 * For example, a document that looks like:
 * {'a': 'aaaa', 'b': { 'c': 12, 'd': true}, e: 'eeee'}
 *
 * Will produce 4 files with the given contents:
 * root.a: aaaa
 * root.b.c: 12
 * root.b.d: true
 * root.e: eeee
 */
public class JsonShredder {

  private final Map<String, PrintStream> files =
      new HashMap<String, PrintStream>();

  private PrintStream getFile(String name) throws IOException {
    PrintStream result = files.get(name);
    if (result == null) {
      result = new PrintStream(new FileOutputStream(name + ".txt"), false,
          StandardCharsets.UTF_8.name());
      files.put(name, result);
    }
    return result;
  }

  private void shredObject(String name, JsonElement json) throws IOException {
    if (json.isJsonPrimitive()) {
      JsonPrimitive primitive = (JsonPrimitive) json;
      getFile(name).println(primitive.getAsString());
    } else if (json.isJsonNull()) {
      // just skip it
    } else if (json.isJsonArray()) {
      for(JsonElement child: ((JsonArray) json)) {
        shredObject(name + ".list", child);
      }
    } else {
      JsonObject obj = (JsonObject) json;
      for(Map.Entry<String,JsonElement> field: obj.entrySet()) {
        String fieldName = field.getKey();
        shredObject(name + "." + fieldName, field.getValue());
      }
    }
  }

  private void close() throws IOException {
    for(Map.Entry<String, PrintStream> file: files.entrySet()) {
      file.getValue().close();
    }
  }

  public static void main(String[] args) throws Exception {
    int count = 0;
    JsonShredder shredder = new JsonShredder();
    for (String filename: args) {
      System.out.println("Reading " + filename);
      System.out.flush();
      java.io.Reader reader;
      FileInputStream inStream = new FileInputStream(filename);
      if (filename.endsWith(".gz")) {
        reader = new InputStreamReader(new GZIPInputStream(inStream),
            StandardCharsets.UTF_8);
      } else {
        reader = new InputStreamReader(inStream, StandardCharsets.UTF_8);
      }
      JsonStreamParser parser = new JsonStreamParser(reader);
      while (parser.hasNext()) {
        count += 1;
        JsonElement item = parser.next();
        shredder.shredObject("root", item);
      }
    }
    shredder.close();
    System.out.println(count + " records read");
    System.out.println();
  }
}
