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

/**
 * API to support adding additional benchmarks to the Driver.
 */
public interface OrcBenchmark {

  /**
   * Get the name of the subcommand to invoke this benchmark.
   * @return a simple string, hopefully lowercase
   */
  String getName();

  /**
   * The human readable description of this benchmark
   * @return
   */
  String getDescription();

  /**
   * Run the benchmark
   * @param args the arguments from the user
   * @throws Exception
   */
  void run(String[] args) throws Exception;
}
