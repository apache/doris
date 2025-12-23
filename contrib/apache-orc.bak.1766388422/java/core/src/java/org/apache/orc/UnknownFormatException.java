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

import com.google.protobuf.TextFormat;
import org.apache.hadoop.fs.Path;

import java.io.IOException;

/**
 * @deprecated This will be removed in the future releases.
 */
@Deprecated
public class UnknownFormatException extends IOException {
  private final Path path;
  private final String versionString;
  private final OrcProto.PostScript postscript;

  public UnknownFormatException(Path path, String versionString,
                                OrcProto.PostScript postscript) {
    super(path + " was written by a future ORC version " +
        versionString + ". This file is not readable by this version of ORC.\n"+
        "Postscript: " + TextFormat.shortDebugString(postscript));
    this.path = path;
    this.versionString = versionString;
    this.postscript = postscript;
  }

  public Path getPath() {
    return path;
  }

  public String getVersionString() {
    return versionString;
  }

  public OrcProto.PostScript getPostscript() {
    return postscript;
  }
}
