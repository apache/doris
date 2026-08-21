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

const loggerName = `org.apache.doris.ui.M10BrowserProbe${Date.now()}`;

test.setTimeout(60_000);

test.afterEach(async ({ page }) => {
  if (page.isClosed()) return;
  await page.evaluate(async (name) => {
    const response = await fetch('/rest/v1/ui/me');
    if (!response.ok) return;
    const envelope = await response.json() as { data?: { csrfToken?: string } };
    const token = envelope.data?.csrfToken;
    if (!token) return;
    await fetch('/rest/v1/ui/log/verbose', {
      method: 'DELETE',
      headers: { 'Content-Type': 'application/json', 'X-Doris-CSRF-Token': token },
      body: JSON.stringify({ name }),
    });
    await fetch('/rest/v1/ui/logout', { method: 'POST', headers: { 'X-Doris-CSRF-Token': token } });
  }, loggerName).catch(() => undefined);
});

test('reads logs and safely adds, refreshes, and deletes a verbose logger', async ({ page }) => {
  await page.goto('/login');
  await page.getByLabel('Username').fill('root');
  await page.getByRole('button', { name: 'Sign in' }).click();
  await page.getByRole('menuitem', { name: /Log/ }).click();

  await expect(page.getByRole('heading', { name: 'Log', exact: true })).toBeVisible();
  await expect(page.getByText('INFO', { exact: true })).toBeVisible();
  await expect(page.locator('.log-viewer')).toBeVisible();
  await page.getByLabel('New verbose logger name').fill(loggerName);
  await page.getByRole('button', { name: 'Add verbose name' }).click();
  await expect(page.getByText(loggerName, { exact: true })).toBeVisible();

  await page.reload();
  await expect(page.getByText(loggerName, { exact: true })).toBeVisible();
  await page.getByRole('button', { name: `Delete ${loggerName}` }).click();
  await page.getByRole('button', { name: 'Delete', exact: true }).click();
  await expect(page.getByText(loggerName, { exact: true })).toHaveCount(0);
});
