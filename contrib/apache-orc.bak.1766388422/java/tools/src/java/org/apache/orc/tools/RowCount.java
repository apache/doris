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

package org.apache.orc.tools;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.LocatedFileStatus;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.RemoteIterator;
import org.apache.orc.OrcFile;
import org.apache.orc.Reader;

import java.io.IOException;

/**
 * Given a set of paths, finds all of the "*.orc" files under them and prints the number of rows in each file.
 */
public class RowCount {
  public static void main(Configuration conf, String[] args) throws IOException {
    int bad = 0;
    for(String root: args) {
      Path rootPath = new Path(root);
      FileSystem fs = rootPath.getFileSystem(conf);
      for(RemoteIterator<LocatedFileStatus> itr = fs.listFiles(rootPath, true); itr.hasNext(); ) {
        LocatedFileStatus status = itr.next();
        if (status.isFile() && status.getPath().getName().endsWith(".orc")) {
          Path filename = status.getPath();
          try (Reader reader = OrcFile.createReader(filename, OrcFile.readerOptions(conf))) {
            System.out.println(String.format("%s %d",
                filename.toString(), reader.getNumberOfRows()));
          } catch (IOException ioe) {
            bad += 1;
            System.err.println("Failed to read " + filename);
          }
        }
      }
    }
    System.exit(bad == 0 ? 0 : 1);
  }

  public static void main(String[] args) throws IOException {
    main(new Configuration(), args);
  }
}
