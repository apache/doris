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

import { QueryClientProvider } from '@tanstack/react-query';
import { ConfigProvider } from 'antd';
import React from 'react';
import ReactDOM from 'react-dom/client';
import { BrowserRouter } from 'react-router-dom';

import { AppRoutes } from './app/AppRoutes';
import { queryClient } from './app/queryClient';
import { resolveRuntimeBasePath } from './runtimeBasePath';
import './styles/global.css';

const rootElement = document.getElementById('root');

if (!rootElement) {
  throw new Error('The application root element is missing.');
}

void resolveRuntimeBasePath().then((basePath) => {
  ReactDOM.createRoot(rootElement).render(
    <React.StrictMode>
      <ConfigProvider
        theme={{
          token: {
            colorPrimary: '#0dbe85',
            colorInfo: '#0dbe85',
            colorText: '#1d2434',
            borderRadius: 0,
            controlHeight: 44,
            fontFamily: '"IBM Plex Sans", "Noto Sans", Arial, sans-serif',
          },
        }}
      >
        <QueryClientProvider client={queryClient}>
          <BrowserRouter basename={basePath || undefined}>
            <AppRoutes />
          </BrowserRouter>
        </QueryClientProvider>
      </ConfigProvider>
    </React.StrictMode>,
  );
});
