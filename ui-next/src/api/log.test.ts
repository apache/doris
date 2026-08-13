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

import { addVerboseName, deleteVerboseName, fetchLog } from './log';

const snapshot = {
  level: 'INFO',
  mode: 'ASYNC',
  verboseNames: [],
  auditNames: ['query'],
  logPath: '/logs/fe.warn.log',
  showingLastBytes: 12,
  contents: 'warning\n',
  contentError: null,
};

function legacySuccess() {
  return new Response(JSON.stringify({ code: 0, data: {
    LogConfiguration: { Level: 'INFO', Mode: 'ASYNC', VerboseNames: '', AuditNames: 'query' },
    LogContents: { logPath: '/logs/fe.warn.log', showingLast: '12 bytes of log', log: '<pre>warning</br></pre>' },
  } }), {
    status: 200,
    headers: { 'Content-Type': 'application/json', 'X-Request-Id': 'req-log' },
  });
}

function mutationSuccess() {
  return new Response(JSON.stringify({ code: 0, data: {} }), {
    status: 200,
    headers: { 'Content-Type': 'application/json' },
  });
}

describe('Log UI API', () => {
  it('reads the structured log snapshot', async () => {
    const fetchSpy = vi.spyOn(globalThis, 'fetch').mockResolvedValue(legacySuccess());
    await expect(fetchLog()).resolves.toEqual(snapshot);
    expect(fetchSpy).toHaveBeenCalledWith('/rest/v1/log', expect.objectContaining({ method: 'GET' }));
  });

  it.each([
    ['add', addVerboseName],
    ['delete', deleteVerboseName],
  ] as const)('%s reuses the legacy form endpoint', async (label, operation) => {
    const fetchSpy = vi.spyOn(globalThis, 'fetch').mockResolvedValue(mutationSuccess());

    await operation('org.apache.doris.M10Probe');

    const init = fetchSpy.mock.calls[0]?.[1];
    const headers = new Headers(init?.headers);
    expect(fetchSpy.mock.calls[0]?.[0]).toBe('/rest/v1/log');
    expect(init?.method).toBe('POST');
    expect(headers.get('Content-Type')).toContain('application/x-www-form-urlencoded');
    expect(init?.body).toBe(`${label === 'add' ? 'add_verbose' : 'del_verbose'}=org.apache.doris.M10Probe`);
  });

  it('rejects invalid names before calling the legacy endpoint', async () => {
    const fetchSpy = vi.spyOn(globalThis, 'fetch');
    await expect(addVerboseName('not a logger name')).rejects.toMatchObject({ code: 'UI_LOG_VERBOSE_INVALID' });
    expect(fetchSpy).not.toHaveBeenCalled();
  });
});
