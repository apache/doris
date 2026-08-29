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

import {Result} from '@src/interfaces/http.interface';

export interface HistoryItem {
    id: string;
    sql: string;
    dbName: string;
    tblName: string;
    success: boolean;
    time: string;
}

interface CacheEntry {
    result: Result<any>;
    dbName: string;
    tblName: string;
    cachedAt: number;
}

const HISTORY_KEY = `doris_sql_history_${location.host}`;
const CACHE_KEY = `doris_sql_cache_${location.host}`;
const HISTORY_LIMIT = 100;
const CACHE_TTL_MS = 5 * 60 * 1000; // 5 minutes
const CACHE_MAX_ENTRIES = 30;
const CACHE_MAX_ROWS = 500; // cap rows cached per query

function safeParse<T>(raw: string | null, fallback: T): T {
    if (!raw) {
        return fallback;
    }
    try {
        return JSON.parse(raw) as T;
    } catch {
        return fallback;
    }
}

// --- normalization / hashing ---------------------------------------------

function normalizeSql(sql: string): string {
    return sql.trim().replace(/\s+/g, ' ');
}

function cacheKey(sql: string, dbName: string): string {
    // simple, stable string key: db scope + normalized sql
    return `${dbName || ''}::${normalizeSql(sql)}`;
}

// --- history --------------------------------------------------------------

export function getHistory(): HistoryItem[] {
    return safeParse<HistoryItem[]>(localStorage.getItem(HISTORY_KEY), []);
}

export function saveHistory(item: Omit<HistoryItem, 'id'>): void {
    const list = getHistory();
    const entry: HistoryItem = {...item, id: `${Date.now()}_${Math.random().toString(36).slice(2, 8)}`};
    list.unshift(entry);
    try {
        localStorage.setItem(HISTORY_KEY, JSON.stringify(list.slice(0, HISTORY_LIMIT)));
    } catch {
        // storage full — drop oldest half and retry once
        try {
            localStorage.setItem(HISTORY_KEY, JSON.stringify(list.slice(0, Math.floor(HISTORY_LIMIT / 2))));
        } catch {
            /* give up silently; history is best-effort */
        }
    }
}

export function removeHistoryItem(id: string): void {
    localStorage.setItem(HISTORY_KEY, JSON.stringify(getHistory().filter(h => h.id !== id)));
}

export function clearHistory(): void {
    localStorage.removeItem(HISTORY_KEY);
}

// --- result cache ---------------------------------------------------------

function readCache(): Record<string, CacheEntry> {
    return safeParse<Record<string, CacheEntry>>(localStorage.getItem(CACHE_KEY), {});
}

function writeCache(map: Record<string, CacheEntry>): void {
    try {
        localStorage.setItem(CACHE_KEY, JSON.stringify(map));
    } catch {
        /* best-effort */
    }
}

export function cacheResult(sql: string, dbName: string, tblName: string, result: Result<any>): void {
    // only cache SELECT-style results that carry a row array
    const rows = result?.data?.data;
    if (!Array.isArray(rows)) {
        return;
    }
    const map = readCache();

    const now = Date.now();
    const live = Object.entries(map).filter(([, v]) => now - v.cachedAt < CACHE_TTL_MS);
    live.sort((a, b) => b[1].cachedAt - a[1].cachedAt);
    const trimmed: Record<string, CacheEntry> = {};
    live.slice(0, CACHE_MAX_ENTRIES - 1).forEach(([k, v]) => (trimmed[k] = v));

    // cap the inner row array, preserve meta/time/type
    const capped: Result<any> = {
        ...result,
        data: {...result.data, data: rows.slice(0, CACHE_MAX_ROWS)},
    };
    trimmed[cacheKey(sql, dbName)] = {result: capped, dbName, tblName, cachedAt: now};
    writeCache(trimmed);
}

export function getCachedResult(sql: string, dbName: string): CacheEntry | null {
    const entry = readCache()[cacheKey(sql, dbName)];
    if (!entry) {
        return null;
    }
    if (Date.now() - entry.cachedAt >= CACHE_TTL_MS) {
        return null;
    }
    return entry;
}

export function clearCache(): void {
    localStorage.removeItem(CACHE_KEY);
}
