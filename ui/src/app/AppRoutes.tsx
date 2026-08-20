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

import { lazy, Suspense } from 'react';
import { Navigate, Route, Routes } from 'react-router-dom';

import { LoginPage } from '../pages/LoginPage';
import { ConfigurationPage } from '../pages/ConfigurationPage';
import { HomePage } from '../pages/HomePage';
import { LogPage } from '../pages/LogPage';
import { SessionsPage } from '../pages/SessionsPage';
import { SystemPage } from '../pages/SystemPage';
import { AuthGate } from './AuthGate';
import { ModulePlaceholder } from './ModulePlaceholder';

const PlaygroundPage = lazy(async () => {
  const module = await import('../pages/PlaygroundPage');
  return { default: module.PlaygroundPage };
});

const QueryProfilesPage = lazy(async () => {
  const module = await import('../pages/QueryProfilesPage');
  return { default: module.QueryProfilesPage };
});

export function AppRoutes() {
  return (
    <Routes>
      <Route path="/login" element={<LoginPage />} />
      <Route element={<AuthGate />}>
        <Route path="/" element={<Navigate to="/home" replace />} />
        <Route path="/home" element={<HomePage />} />
        <Route
          path="/playground/*"
          element={<Suspense fallback={<main className="full-page-state">Loading Playground…</main>}><PlaygroundPage /></Suspense>}
        />
        <Route path="/system/*" element={<SystemPage />} />
        <Route path="/log" element={<LogPage />} />
        <Route
          path="/query-profiles/:profileId?"
          element={<Suspense fallback={<main className="full-page-state">Loading Query Profiles…</main>}><QueryProfilesPage /></Suspense>}
        />
        <Route path="/sessions" element={<SessionsPage />} />
        <Route path="/configuration" element={<ConfigurationPage />} />
        <Route path="*" element={<ModulePlaceholder name="Page not found" milestone="No module" />} />
      </Route>
    </Routes>
  );
}
