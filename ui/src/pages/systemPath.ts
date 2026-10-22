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

import { normalizeProcPath } from '../api/operations';

export interface ProcBreadcrumb {
  label: string;
  path: string;
}

export function systemRoute(path: string): string {
  return `/system?${new URLSearchParams({ path: normalizeProcPath(path) }).toString()}`;
}

export function parentProcPath(path: string): string | null {
  const normalized = normalizeProcPath(path);
  if (normalized === '/') return null;
  const segments = normalized.split('/').filter(Boolean);
  segments.pop();
  return segments.length === 0 ? '/' : `/${segments.join('/')}`;
}

export function procBreadcrumbs(path: string): ProcBreadcrumb[] {
  const normalized = normalizeProcPath(path);
  const result: ProcBreadcrumb[] = [{ label: 'Root', path: '/' }];
  const segments = normalized.split('/').filter(Boolean);
  segments.forEach((segment, index) => {
    result.push({ label: segment, path: `/${segments.slice(0, index + 1).join('/')}` });
  });
  return result;
}
