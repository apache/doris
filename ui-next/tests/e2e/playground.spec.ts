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

const SESSION_KEY = 'doris.ui.web-sql-session.v1';

test.setTimeout(90_000);

test.afterEach(async ({ page }) => {
  if (page.isClosed()) return;
  await page.evaluate(async () => {
    const response = await fetch('/rest/v1/ui/me');
    if (!response.ok) return;
    const me: unknown = await response.json();
    if (!me || typeof me !== 'object' || !('data' in me)
      || !me.data || typeof me.data !== 'object' || !('csrfToken' in me.data)
      || typeof me.data.csrfToken !== 'string') return;
    await fetch('/rest/v1/ui/logout', {
      method: 'POST',
      headers: { 'X-Doris-CSRF-Token': me.data.csrfToken },
    });
  }).catch(() => undefined);
});

async function signIn(page: Page) {
  await page.goto('/login');
  await page.getByLabel('Username').fill('root');
  await page.getByRole('button', { name: 'Sign in' }).click();
  await expect(page).toHaveURL(/\/home$/);
}

async function openPlayground(page: Page) {
  await page.getByRole('menuitem', { name: /Playground/ }).click();
  await expect(page.getByRole('heading', { name: 'Playground' })).toBeVisible();
  await expect(page.locator('.session-ready')).toBeVisible();
  const sessionDetails = page.getByLabel('SQL session details');
  await expect(sessionDetails).toContainText('Connection status');
  await expect(sessionDetails).toContainText('Session ID');
  const storedId = await page.evaluate((key) => sessionStorage.getItem(key), SESSION_KEY);
  expect(storedId).toBeTruthy();
  await expect(sessionDetails.locator('code')).toHaveText(storedId);
}

async function setSql(page: Page, statement: string) {
  const editor = page.locator('.cm-content');
  await editor.click();
  await editor.fill(statement);
}

async function runSql(page: Page, statement: string, resultNumber: number) {
  await setSql(page, statement);
  await page.getByRole('button', { name: /Run selection \/ all/ }).click();
  await expect(page.getByText(`Result ${resultNumber}`, { exact: true })).toBeVisible({ timeout: 20_000 });
}

test('keeps SQL state across statements and refresh, while cloned storage gets an independent session', async ({ page, context }) => {
  await page.addInitScript(() => {
    Object.defineProperty(Crypto.prototype, 'randomUUID', {
      configurable: true,
      value: undefined,
    });
  });
  const forbiddenRequests: string[] = [];
  page.on('request', (request) => {
    if (/(upload|stream[_-]?load|data[_-]?import)/i.test(request.url())) forbiddenRequests.push(request.url());
  });

  await signIn(page);
  await expect.poll(() => page.evaluate(() => typeof crypto.randomUUID)).toBe('undefined');
  await openPlayground(page);
  await runSql(page, 'SET @m7_value = 41', 1);
  await runSql(page, 'SELECT @m7_value + 1 AS answer', 2);
  await expect(page.getByText('42', { exact: true })).toBeVisible();
  await runSql(page, 'USE tpcds', 3);
  await runSql(page, 'SELECT DATABASE() AS current_database', 4);
  await expect(page.getByText('tpcds', { exact: true }).last()).toBeVisible();

  const originalId = await page.evaluate((key) => sessionStorage.getItem(key), SESSION_KEY);
  expect(originalId).toBeTruthy();
  if (!originalId) throw new Error('The original tab did not store a SQL session id.');
  await page.reload();
  await expect(page.locator('.session-ready')).toBeVisible();
  await expect.poll(() => page.evaluate((key) => sessionStorage.getItem(key), SESSION_KEY)).toBe(originalId);
  await runSql(page, 'SELECT @m7_value AS value_after_refresh', 1);
  await expect(page.getByText('41', { exact: true })).toBeVisible();

  const cloned = await context.newPage();
  await cloned.goto('/home');
  await cloned.evaluate(([key, id]) => sessionStorage.setItem(key, id), [SESSION_KEY, originalId]);
  await cloned.goto('/playground');
  await expect(cloned.locator('.session-ready')).toBeVisible();
  const clonedId = await cloned.evaluate((key) => sessionStorage.getItem(key), SESSION_KEY);
  expect(clonedId).toBeTruthy();
  expect(clonedId).not.toBe(originalId);
  await cloned.getByRole('button', { name: 'Close session' }).click();
  await expect(cloned.getByText('The SQL session is closed')).toBeVisible();
  await cloned.close();

  expect(forbiddenRequests).toEqual([]);
});

test('serializes Run, cancels, resets, rebuilds a missing session, and closes explicitly', async ({ page }) => {
  await signIn(page);
  await openPlayground(page);
  const runButton = page.getByRole('button', { name: /Run selection \/ all/ });

  await setSql(page, 'SELECT SLEEP(5)');
  await runButton.click();
  await expect(runButton).toBeDisabled();
  const cancelButton = page.getByRole('button', { name: 'Cancel' });
  await expect(cancelButton).toBeEnabled();
  await cancelButton.click();
  await expect(page.getByText(/Cancel was requested/)).toBeVisible();
  await expect(runButton).toBeEnabled({ timeout: 20_000 });

  await runSql(page, 'SET @m7_reset_value = 9', 1);
  await page.getByRole('button', { name: 'Reset connection' }).click();
  await expect(page.getByText(/connection was reset/i)).toBeVisible();
  await runSql(page, 'SELECT @m7_reset_value AS reset_value', 1);
  await expect(page.locator('.sql-null').last()).toHaveText('NULL');

  const expiredId = await page.evaluate((key) => sessionStorage.getItem(key), SESSION_KEY);
  if (!expiredId) throw new Error('The page did not store a SQL session id.');
  const closeStatus = await page.evaluate(async (id) => {
    const me: unknown = await (await fetch('/rest/v1/ui/me')).json();
    if (!me || typeof me !== 'object' || !('data' in me)
      || !me.data || typeof me.data !== 'object' || !('csrfToken' in me.data)
      || typeof me.data.csrfToken !== 'string') {
      throw new Error('The UI session response did not contain a CSRF token.');
    }
    return (await fetch(`/rest/v1/ui/sql-sessions/${encodeURIComponent(id)}`, {
      method: 'DELETE',
      headers: { 'X-Doris-CSRF-Token': me.data.csrfToken },
    })).status;
  }, expiredId);
  expect(closeStatus).toBe(200);
  await runSql(page, 'SELECT 7 AS rebuilt', 2);
  await expect(page.locator('.query-result').getByRole('cell').getByText('7', { exact: true })).toBeVisible();
  const rebuiltId = await page.evaluate((key) => sessionStorage.getItem(key), SESSION_KEY);
  expect(rebuiltId).not.toBe(expiredId);

  await page.getByRole('button', { name: 'Close session' }).click();
  await expect(page.getByText('The SQL session is closed')).toBeVisible();
  await page.getByRole('button', { name: 'Open session' }).click();
  await expect(page.locator('.session-ready')).toBeVisible();
  const reopenedId = await page.evaluate((key) => sessionStorage.getItem(key), SESSION_KEY);
  expect(reopenedId).not.toBe(rebuiltId);
});

test('loads the object explorer as a lazy database and table tree', async ({ page }) => {
  const forbiddenRequests: string[] = [];
  page.on('request', (request) => {
    if (/(upload|stream[_-]?load|data[_-]?import)/i.test(request.url())) forbiddenRequests.push(request.url());
  });

  await signIn(page);
  await openPlayground(page);
  const tpcds = page.locator('.ant-tree-title', { hasText: 'tpcds' }).filter({ hasText: /^tpcds$/ });
  await expect(tpcds).toBeVisible({ timeout: 20_000 });
  await expect(page.locator('.ant-tree-title', { hasText: /^catalog_page$/ })).toHaveCount(0);

  await tpcds.click();
  const catalogPage = page.locator('.ant-tree-title', { hasText: /^catalog_page$/ });
  await expect(catalogPage).toBeVisible({ timeout: 20_000 });
  await catalogPage.click();
  await expect(page.locator('.schema-list > button').first()).toBeVisible({ timeout: 20_000 });

  const firstColumn = page.locator('.schema-list > button').first();
  const firstColumnName = await firstColumn.locator('span').first().textContent();
  await firstColumn.click();
  await expect(page.locator('.cm-content')).toContainText(`\`${firstColumnName}\``);

  await page.getByRole('button', { name: 'Query table' }).click();
  await expect(page.locator('.cm-content')).toContainText('SELECT *');
  await expect(page.locator('.cm-content')).toContainText('`internal`.`tpcds`.`catalog_page`');

  const search = page.getByLabel('Search databases and loaded tables');
  await search.fill('catalog_page');
  await expect(catalogPage).toBeVisible();
  await page.getByRole('button', { name: 'Refresh object explorer' }).click();
  await expect(catalogPage).toHaveCount(0);
  expect(forbiddenRequests).toEqual([]);
});
