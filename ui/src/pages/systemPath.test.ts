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

import { parentProcPath, procBreadcrumbs, systemRoute } from './systemPath';

describe('System Proc navigation', () => {
  it('builds root and nested breadcrumbs', () => {
    expect(procBreadcrumbs('/')).toEqual([{ label: 'Root', path: '/' }]);
    expect(procBreadcrumbs('/catalogs/internal')).toEqual([
      { label: 'Root', path: '/' },
      { label: 'catalogs', path: '/catalogs' },
      { label: 'internal', path: '/catalogs/internal' },
    ]);
  });

  it('builds parent paths without escaping root', () => {
    expect(parentProcPath('/')).toBeNull();
    expect(parentProcPath('/catalogs')).toBe('/');
    expect(parentProcPath('/catalogs/internal')).toBe('/catalogs');
  });

  it('encodes a Proc path into a browser route', () => {
    expect(systemRoute('/catalogs/internal db')).toBe('/system?path=%2Fcatalogs%2Finternal+db');
  });
});
