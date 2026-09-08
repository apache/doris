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

import type { ProfileGraphIR } from './profileGraphTypes';
import { MAX_PARSER_BYTES, ProfileParserError, type ProfileParserErrorCode } from './profileParser';
import type { ProfileParseRequest, ProfileParseResponse } from './profileParserProtocol';

export const DEFAULT_PROFILE_PARSE_TIMEOUT_MS = 8_000;

export interface ProfileParserWorker {
  onmessage: ((event: MessageEvent<ProfileParseResponse>) => void) | null;
  onerror: ((event: ErrorEvent) => void) | null;
  postMessage(message: ProfileParseRequest): void;
  terminate(): void;
}

export type ProfileParserWorkerFactory = () => ProfileParserWorker;

export interface ProfileParseOperation {
  promise: Promise<ProfileGraphIR>;
  cancel(): void;
}

export function profileParserErrorMessage(code: ProfileParserErrorCode): string {
  if (code === 'DAG_TOO_LARGE') return 'This execution graph is too large to display.';
  if (code === 'DAG_UNAVAILABLE') return 'An execution graph is not available for this Profile.';
  return 'The execution graph could not be generated.';
}

function requestId(): string {
  if (typeof crypto.randomUUID === 'function') return crypto.randomUUID();
  const bytes = crypto.getRandomValues(new Uint8Array(16));
  return Array.from(bytes, (byte) => byte.toString(16).padStart(2, '0')).join('');
}

export function startProfileParse(
  text: string,
  createWorker: ProfileParserWorkerFactory,
  timeoutMs = DEFAULT_PROFILE_PARSE_TIMEOUT_MS,
): ProfileParseOperation {
  const id = requestId();
  let worker: ProfileParserWorker | null = null;
  let settled = false;
  let rejectPromise: ((reason: unknown) => void) | null = null;
  let timeout: ReturnType<typeof setTimeout> | null = null;

  const finish = () => {
    if (timeout !== null) clearTimeout(timeout);
    timeout = null;
    worker?.terminate();
    worker = null;
  };
  const promise = new Promise<ProfileGraphIR>((resolve, reject) => {
    rejectPromise = reject;
    if (new TextEncoder().encode(text).byteLength > MAX_PARSER_BYTES) {
      settled = true;
      reject(new ProfileParserError('DAG_TOO_LARGE', profileParserErrorMessage('DAG_TOO_LARGE')));
      return;
    }
    try {
      worker = createWorker();
    } catch {
      settled = true;
      reject(new ProfileParserError('DAG_PARSE_FAILED', profileParserErrorMessage('DAG_PARSE_FAILED')));
      return;
    }
    worker.onmessage = (event) => {
      if (settled || event.data.requestId !== id) return;
      settled = true;
      finish();
      if (event.data.type === 'PARSE_SUCCESS') resolve(event.data.graph);
      else reject(new ProfileParserError(event.data.code, event.data.message || profileParserErrorMessage(event.data.code)));
    };
    worker.onerror = () => {
      if (settled) return;
      settled = true;
      finish();
      reject(new ProfileParserError('DAG_PARSE_FAILED', profileParserErrorMessage('DAG_PARSE_FAILED')));
    };
    timeout = setTimeout(() => {
      if (settled) return;
      settled = true;
      finish();
      reject(new ProfileParserError('DAG_PARSE_FAILED', 'The execution graph parser timed out.'));
    }, timeoutMs);
    worker.postMessage({ type: 'PARSE_PROFILE', requestId: id, text });
  });

  return {
    promise,
    cancel() {
      if (settled) return;
      settled = true;
      finish();
      rejectPromise?.(new DOMException('The operation was aborted.', 'AbortError'));
    },
  };
}
