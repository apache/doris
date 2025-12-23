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

package org.apache.orc.bench.core;

import io.airlift.compress.snappy.SnappyCodec;
import org.apache.hadoop.fs.Path;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.util.zip.GZIPInputStream;
import java.util.zip.GZIPOutputStream;

/**
 * Enum for handling the compression codecs for the benchmark
 */
public enum CompressionKind {
  NONE("none"),
  ZLIB("gz"),
  SNAPPY("snappy"),
  ZSTD("zstd");

  CompressionKind(String extension) {
    this.extension = extension;
  }

  private final String extension;

  public String getExtension() {
    return extension;
  }

  public OutputStream create(OutputStream out) throws IOException {
    switch (this) {
      case NONE:
        return out;
      case ZLIB:
        return new GZIPOutputStream(out);
      case SNAPPY:
        return new SnappyCodec().createOutputStream(out);
      default:
        throw new IllegalArgumentException("Unhandled kind " + this);
    }
  }

  public InputStream read(InputStream in) throws IOException {
    switch (this) {
      case NONE:
        return in;
      case ZLIB:
        return new GZIPInputStream(in);
      case SNAPPY:
        return new SnappyCodec().createInputStream(in);
      default:
        throw new IllegalArgumentException("Unhandled kind " + this);
    }
  }

  public static CompressionKind fromPath(Path path) {
    String name = path.getName();
    int lastDot = name.lastIndexOf('.');
    if (lastDot >= 0) {
      String ext = name.substring(lastDot);
      for (CompressionKind value : values()) {
        if (ext.equals("." + value.getExtension())) {
          return value;
        }
      }
    }
    return NONE;
  }

  public static CompressionKind fromExtension(String extension) {
    for (CompressionKind value: values()) {
      if (value.extension.equals(extension)) {
        return value;
      }
    }
    throw new IllegalArgumentException("Unknown compression " + extension);
  }
}
