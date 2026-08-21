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

import { expect, type Page, test } from '@playwright/test';

test.setTimeout(90_000);

async function signIn(page: Page) {
  await page.goto('/login');
  await page.getByLabel('Username').fill('root');
  await page.getByRole('button', { name: 'Sign in' }).click();
  await expect(page).toHaveURL(/\/home$/);
}

test.afterEach(async ({ page }) => {
  if (page.isClosed()) return;
  await page.evaluate(async () => {
    const response = await fetch('/rest/v1/ui/me');
    if (!response.ok) return;
    const envelope = await response.json() as { data?: { csrfToken?: string } };
    const token = envelope.data?.csrfToken;
    if (!token) return;
    await fetch('/rest/v1/ui/logout', {
      method: 'POST',
      headers: { 'X-Doris-CSRF-Token': token },
    });
  }).catch(() => undefined);
});

test('browses, refreshes, and history-navigates the live System Proc tree', async ({ page }) => {
  await signIn(page);
  await page.getByRole('menuitem', { name: /System/ }).click();
  await expect(page.getByRole('heading', { name: '/' })).toBeVisible();

  await page.getByRole('link', { name: 'backends', exact: true }).click();
  await expect(page.getByRole('heading', { name: '/backends' })).toBeVisible();
  const backendLink = page.locator('.operations-table tbody a').first();
  const backendId = (await backendLink.textContent())?.trim();
  expect(backendId).toBeTruthy();
  await backendLink.click();
  await expect(page.getByRole('heading', { name: `/backends/${backendId}` })).toBeVisible();
  await expect(page.getByRole('columnheader', { name: 'RootPath' })).toBeVisible();

  await page.reload();
  await expect(page.getByRole('heading', { name: `/backends/${backendId}` })).toBeVisible();
  await page.goBack();
  await expect(page.getByRole('heading', { name: '/backends' })).toBeVisible();
  await page.goForward();
  await expect(page.getByRole('heading', { name: `/backends/${backendId}` })).toBeVisible();
  await page.getByRole('button', { name: 'Parent directory' }).click();
  await expect(page.getByRole('heading', { name: '/backends' })).toBeVisible();
  await page.getByRole('button', { name: 'Refresh' }).click();
  await expect(page.getByRole('columnheader', { name: 'BackendId' })).toBeVisible();

  await page.goto('/system?path=%2Fdoes-not-exist-m9');
  await expect(page.getByRole('alert')).toBeVisible();
  await expect(page.getByRole('button', { name: 'Retry' })).toBeVisible();
});

test('shows and filters the live read-only Sessions table', async ({ page }) => {
  await signIn(page);
  await page.getByRole('menuitem', { name: /Playground/ }).click();
  await expect(page.locator('.session-ready')).toBeVisible({ timeout: 20_000 });
  await page.getByRole('menuitem', { name: /Sessions/ }).click();
  await expect(page.getByRole('heading', { name: 'Sessions', exact: true })).toBeVisible();
  await expect(page.getByText('Active sessions')).toBeVisible();
  await expect(page.getByRole('columnheader', { name: 'Id', exact: true })).toBeVisible();
  await expect(page.getByRole('columnheader', { name: 'User', exact: true })).toBeVisible();
  await expect(page.getByRole('columnheader', { name: 'Info', exact: true })).toBeVisible();

  await page.getByRole('searchbox', { name: 'Filter table' }).fill('root');
  await expect(page.locator('.operations-table tbody')).toContainText('root');
  await page.getByRole('button', { name: 'Refresh' }).click();
  await expect(page.locator('.operations-table tbody')).toContainText('root');
  await expect(page.getByRole('button', { name: /Kill/i })).toHaveCount(0);
});
