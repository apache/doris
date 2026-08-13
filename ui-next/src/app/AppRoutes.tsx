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
        <Route path="/query-profiles/*" element={<ModulePlaceholder name="Query Profiles" milestone="M10" />} />
        <Route path="/sessions" element={<SessionsPage />} />
        <Route path="/configuration" element={<ModulePlaceholder name="Configuration" milestone="M14" />} />
        <Route path="*" element={<ModulePlaceholder name="Page not found" milestone="No module" />} />
      </Route>
    </Routes>
  );
}
