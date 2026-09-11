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

declare global {
  interface Window {
    __DORIS_BASE_PATH__?: string;
  }
}

export function runtimeBasePath(): string {
  const path = window.__DORIS_BASE_PATH__ ?? '';
  if (!path || path === '/') return '';
  return `/${path.split('/').filter(Boolean).join('/')}`;
}

function normalizeBasePath(path: string): string {
  if (!path || path === '/') return '';
  return `/${path.split('/').filter(Boolean).join('/')}`;
}

export async function resolveRuntimeBasePath(): Promise<string> {
  const inferred = runtimeBasePath();
  try {
    const response = await fetch(`${inferred}/api/basepath`, { credentials: 'same-origin' });
    if (!response.ok) return inferred;
    const envelope = await response.json() as {
      data?: { enable?: boolean; path?: string };
    };
    if (!envelope.data?.enable || typeof envelope.data.path !== 'string') return inferred;
    const configured = normalizeBasePath(envelope.data.path);
    window.__DORIS_BASE_PATH__ = configured;
    const baseElement = document.querySelector('base');
    if (baseElement) baseElement.href = `${window.location.origin}${configured}/`;
    return configured;
  } catch {
    return inferred;
  }
}

export function withRuntimeBasePath(path: string): string {
  if (!path.startsWith('/')) return path;
  return `${runtimeBasePath()}${path}`;
}
