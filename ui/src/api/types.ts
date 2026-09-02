// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements.  See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership.  The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License.  You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.

export interface UiMe {
  user: string;
  csrfToken: string;
}

export interface UiVersionInfo {
  version: string;
  git: string;
  buildInfo: string;
  buildTime: string;
  features: string;
}

export interface UiNodeTable {
  columnNames: string[];
  rows: string[][];
}

export interface UiLogSnapshot {
  level: string;
  mode: string;
  verboseNames: string[];
  auditNames: string[];
  logPath: string;
  showingLastBytes: number;
  contents: string;
  contentError: string | null;
}

export interface WebSqlSessionInfo {
  sessionId: string;
  createdAtMillis: number;
  lastAccessMillis: number;
}

export interface WebSqlColumn {
  name: string;
  type: string;
}

export interface WebSqlExecutionResult {
  columns: WebSqlColumn[];
  rows: unknown[][];
  affectedRows: number;
  elapsedTimeMs: number;
  queryId: string | null;
  warnings: string[];
  catalog: string | null;
  database: string | null;
  truncated: boolean;
}

export interface WebSqlCancelResult {
  cancelRequested: boolean;
}

export interface WebSqlCloseResult {
  closed: boolean;
}

export interface UiErrorBody {
  code: string;
  message: string;
  requestId?: string;
  details?: unknown;
}
