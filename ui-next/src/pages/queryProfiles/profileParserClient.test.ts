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

import { startProfileParse, type ProfileParserWorker } from './profileParserClient';
import type { ProfileGraphIR } from './profileGraphTypes';
import type { ProfileParseRequest, ProfileParseResponse } from './profileParserProtocol';

class FakeWorker implements ProfileParserWorker {
  onmessage: ((event: MessageEvent<ProfileParseResponse>) => void) | null = null;
  onerror: ((event: ErrorEvent) => void) | null = null;
  request: ProfileParseRequest | null = null;
  terminate = vi.fn();
  postMessage(message: ProfileParseRequest) { this.request = message; }
}

describe('Profile parser Worker client', () => {
  it('ignores a stale response and resolves the matching request', async () => {
    const worker = new FakeWorker();
    const operation = startProfileParse('small', () => worker, 1_000);
    worker.onmessage?.(new MessageEvent<ProfileParseResponse>('message', {
      data: { type: 'PARSE_FAILURE', requestId: 'stale', code: 'DAG_PARSE_FAILED', message: 'stale' },
    }));
    const graph = { schemaVersion: '1.0' } as ProfileGraphIR;
    worker.onmessage?.(new MessageEvent<ProfileParseResponse>('message', {
      data: { type: 'PARSE_SUCCESS', requestId: worker.request?.requestId ?? '', graph },
    }));
    await expect(operation.promise).resolves.toBe(graph);
    expect(worker.terminate).toHaveBeenCalledOnce();
  });

  it('terminates the Worker when cancelled', async () => {
    const worker = new FakeWorker();
    const operation = startProfileParse('small', () => worker, 1_000);
    operation.cancel();
    await expect(operation.promise).rejects.toMatchObject({ name: 'AbortError' });
    expect(worker.terminate).toHaveBeenCalledOnce();
  });

  it('terminates and reports a timeout', async () => {
    vi.useFakeTimers();
    const worker = new FakeWorker();
    const operation = startProfileParse('small', () => worker, 10);
    const assertion = expect(operation.promise).rejects.toThrow('timed out');
    await vi.advanceTimersByTimeAsync(11);
    await assertion;
    expect(worker.terminate).toHaveBeenCalledOnce();
    vi.useRealTimers();
  });
});
