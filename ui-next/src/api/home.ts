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

import { useQuery } from '@tanstack/react-query';

import { uiRequest } from './client';
import type { UiNodeTable, UiVersionInfo } from './types';

export async function fetchVersion(): Promise<UiVersionInfo> {
  return (await uiRequest<UiVersionInfo>('/rest/v1/ui/home/version')).data;
}

export async function fetchFrontends(): Promise<UiNodeTable> {
  return (await uiRequest<UiNodeTable>('/rest/v1/ui/nodes/frontends')).data;
}

export async function fetchBackends(): Promise<UiNodeTable> {
  return (await uiRequest<UiNodeTable>('/rest/v1/ui/nodes/backends')).data;
}

export function useVersion() {
  return useQuery({ queryKey: ['ui', 'home', 'version'], queryFn: fetchVersion });
}

export function useFrontends(enabled: boolean) {
  return useQuery({
    queryKey: ['ui', 'nodes', 'frontends'],
    queryFn: fetchFrontends,
    enabled,
    refetchInterval: false,
  });
}

export function useBackends(enabled: boolean) {
  return useQuery({
    queryKey: ['ui', 'nodes', 'backends'],
    queryFn: fetchBackends,
    enabled,
    refetchInterval: false,
  });
}
