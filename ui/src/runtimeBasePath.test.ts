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

import { resolveRuntimeBasePath, runtimeBasePath, withRuntimeBasePath } from './runtimeBasePath';

describe('runtime base path', () => {
  afterEach(() => {
    delete window.__DORIS_BASE_PATH__;
    vi.restoreAllMocks();
  });

  it('normalizes the inferred prefix for routes and API calls', () => {
    window.__DORIS_BASE_PATH__ = '//doris//';

    expect(runtimeBasePath()).toBe('/doris');
    expect(withRuntimeBasePath('/rest/v1/ui/me')).toBe('/doris/rest/v1/ui/me');
  });

  it('honors the configured extra base path returned by the FE', async () => {
    window.__DORIS_BASE_PATH__ = '/inferred';
    vi.spyOn(globalThis, 'fetch').mockResolvedValue(new Response(JSON.stringify({
      data: { enable: true, path: '/configured/' },
    }), { status: 200, headers: { 'Content-Type': 'application/json' } }));

    await expect(resolveRuntimeBasePath()).resolves.toBe('/configured');
    expect(fetch).toHaveBeenCalledWith('/inferred/api/basepath', { credentials: 'same-origin' });
    expect(runtimeBasePath()).toBe('/configured');
  });
});
