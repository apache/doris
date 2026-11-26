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

import { fetchBackends, fetchFrontends, fetchVersion } from './home';

function json(data: unknown) {
  return new Response(JSON.stringify({ code: 0, data }), {
    headers: { 'Content-Type': 'application/json', 'X-Request-Id': 'req-home' },
  });
}

describe('Home API', () => {
  it('adapts the existing hardware and System APIs', async () => {
    const fetchSpy = vi.spyOn(globalThis, 'fetch')
      .mockResolvedValueOnce(json({ VersionInfo: { Version: '4.0', Git: 'abc', BuildInfo: 'builder', BuildTime: 'today', Features: 'x' } }))
      .mockResolvedValueOnce(json({ column_names: ['Name', 'FutureField'], rows: [{ Name: 'fe-1', FutureField: 'kept' }] }))
      .mockResolvedValueOnce(json({ column_names: ['BackendId'], rows: [{ BackendId: '10001' }] }));

    await expect(fetchVersion()).resolves.toMatchObject({ version: '4.0' });
    await expect(fetchFrontends()).resolves.toEqual({ columnNames: ['Name', 'FutureField'], rows: [['fe-1', 'kept']] });
    await expect(fetchBackends()).resolves.toEqual({ columnNames: ['BackendId'], rows: [['10001']] });

    expect(fetchSpy.mock.calls.map(([path]) => path)).toEqual([
      '/rest/v1/hardware_info/fe/version',
      '/rest/v1/system?path=%2Ffrontends',
      '/rest/v1/system?path=%2Fbackends',
    ]);
  });
});
