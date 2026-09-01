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

import { expect, test } from '@playwright/test';

test('guards protected routes and reports invalid credentials', async ({ page }) => {
  await page.goto('/');
  await expect(page.getByRole('heading', { name: 'Sign in' })).toBeVisible();

  await page.goto('/unknown-route');
  await expect(page.getByRole('heading', { name: 'Sign in' })).toBeVisible();

  await page.getByLabel('Username').fill('root');
  await page.getByLabel('Password').fill('not-the-root-password');
  await page.getByRole('button', { name: 'Sign in' }).click();
  await expect(page.getByText('Sign-in failed. Check the username and password.')).toBeVisible();
});

test('signs in, refreshes Home, inspects nodes, and signs out', async ({ page }) => {
  await page.goto('/login');
  await page.getByLabel('Username').fill('root');
  await page.getByRole('button', { name: 'Sign in' }).click();

  await expect(page).toHaveURL(/\/home$/);
  await expect(page.getByRole('heading', { name: 'Cluster Overview' })).toBeVisible();
  await expect(page.locator('.version-grid .ant-descriptions-item-content').first()).not.toHaveText('—');
  await expect(page.getByText(/signed in with an empty password/i)).toBeVisible();
  await expect(page.getByRole('tab', { name: /Frontends \(1\)/ })).toBeVisible();

  await page.reload();
  await expect(page.getByRole('heading', { name: 'Cluster Overview' })).toBeVisible();
  await expect(page.getByText('127.0.0.1').first()).toBeVisible();

  await page.getByLabel('Search frontends').fill('127.0.0.1');
  await page.getByText('127.0.0.1').first().click();
  await expect(page.getByText('Frontend details')).toBeVisible();
  await page.getByRole('button', { name: 'Close', exact: true }).click();

  await page.getByRole('tab', { name: /Backends \(1\)/ }).click();
  await expect(page.locator('.node-panel:visible .node-table tbody tr.node-row').first()).toBeVisible();
  await page.getByRole('button', { name: 'Refresh' }).click();

  await page.getByRole('button', { name: 'Sign out' }).click();
  await expect(page.getByRole('heading', { name: 'Sign in' })).toBeVisible();
  await page.goto('/home');
  await expect(page.getByRole('heading', { name: 'Sign in' })).toBeVisible();
});

test('proxies relative API requests to the local Doris FE', async ({ request }) => {
  const response = await request.get('/api/bootstrap');
  expect(response.ok()).toBe(true);
  await expect(response.json()).resolves.toMatchObject({ code: 0 });
});
