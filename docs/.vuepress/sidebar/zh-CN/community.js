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
  "team",
  "gitter",
  "subscribe-mail-list",
  "feedback",
  {
    title: "贡献指南",
    directoryPath: "how-to-contribute/",
    initialOpenGroupIndex: -1,
    children: [
      "how-to-contribute",
      "contributor-guide",
      "how-to-be-a-committer",
      "commit-format-specification",
      "pull-request",
    ],
  },
  {
    title: "版本发布与校验",
    directoryPath: "release-and-verify/",
    initialOpenGroupIndex: -1,
    children: [
      "release-prepare",
      "release-doris-core",
      "release-doris-connectors",
      "release-doris-manager",
      "release-complete",
      "release-verify",
    ],
  },
  "security",
]