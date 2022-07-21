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
module.exports =  [
  {
    title: "设计文档",
    directoryPath: "design/",
    initialOpenGroupIndex: -1,
    children: [
      "doris_storage_optimization",
      "grouping_sets_design",
      "metadata-design",
      "spark_load",
    ],
  }, 
  {
    title: "开发者手册",
    directoryPath: "developer-guide/",
    initialOpenGroupIndex: -1,
    children: [
      "debug-tool",
      "benchmark-tool",
      "docker-dev",
      "fe-eclipse-dev",
      "fe-idea-dev",
      "fe-vscode-dev",
      "be-vscode-dev",
      "java-format-code",
      "cpp-format-code",
      "cpp-diagnostic-code",
      "how-to-share-blogs",
      "bitmap-hll-file-format",
      "regression-testing",
      "github-checks"
    ],
  },    
]
