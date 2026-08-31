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

/// <reference lib="webworker" />

import { parseProfileText, ProfileParserError } from './profileParser';
import type { ProfileParseRequest } from './profileParserProtocol';

const workerScope = self as unknown as DedicatedWorkerGlobalScope;

workerScope.onmessage = (event: MessageEvent<ProfileParseRequest>) => {
  if (event.data?.type !== 'PARSE_PROFILE') return;
  const { requestId, text } = event.data;
  try {
    workerScope.postMessage({ type: 'PARSE_SUCCESS', requestId, graph: parseProfileText(text) });
  } catch (reason) {
    const error = reason instanceof ProfileParserError
      ? reason
      : new ProfileParserError('DAG_PARSE_FAILED', 'The execution graph could not be generated.');
    workerScope.postMessage({ type: 'PARSE_FAILURE', requestId, code: error.code, message: error.message });
  }
};
