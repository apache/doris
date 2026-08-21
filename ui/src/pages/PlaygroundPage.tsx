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

import { sql } from '@codemirror/lang-sql';
import { EditorView } from '@codemirror/view';
import CodeMirror from '@uiw/react-codemirror';
import {
  Alert,
  Button,
  Empty,
  Input,
  Select,
  Spin,
  Table,
  Tabs,
  Tag,
  Tooltip,
  Tree,
  message,
} from 'antd';
import type { ColumnsType } from 'antd/es/table';
import type { DataNode } from 'antd/es/tree';
import type { Key } from 'react';
import { useCallback, useEffect, useMemo, useRef, useState } from 'react';
import { format } from 'sql-formatter';

import { UiApiError } from '../api/client';
import type { WebSqlExecutionResult } from '../api/types';
import { adaptCatalogs, adaptSchema, adaptSingleNameColumn, type CatalogItem, type SchemaColumn } from './playground/metadataAdapter';
import { executableSql, qualifiedName, quoteIdentifier, sqlStatements, statementRangeAt, type SqlSelection } from './playground/sqlSelection';
import { clearHistory, loadHistory, MAX_RESULT_TABS, saveHistory } from './playground/historyStorage';
import { useWebSqlSession } from './playground/useWebSqlSession';

const INITIAL_SQL = `SELECT COUNT(*) AS row_count
FROM tpcds.store_sales;`;

const QUERY_TABLE_LIMITS = [10, 50, 100, 500, 1000, 5000];
const DEFAULT_QUERY_TABLE_LIMIT = 100;
interface QueryResultTab {
  key: string;
  label: string;
  sql: string;
  result: WebSqlExecutionResult;
}

interface LogMessage {
  id: number;
  tone: 'info' | 'success' | 'error';
  timestamp: string;
  createdAt: number;
  text: string;
}

interface ExplorerTreeNode extends DataNode {
  nodeType: 'database' | 'table';
  database: string;
  table?: string;
  children?: ExplorerTreeNode[];
}

interface SchemaInsertion {
  document: string;
  position: number;
  mode: 'empty' | 'before-existing';
  statementFrom: number;
  statementTo: number;
}

function insertSchemaColumnIntoSql(
  document: string,
  column: string,
  previous: SchemaInsertion | null,
  cursorOffset: number,
): { document: string; insertion: SchemaInsertion } | null {
  let insertion = previous?.document === document
    && cursorOffset >= previous.statementFrom && cursorOffset <= previous.statementTo ? previous : null;
  const continued = insertion !== null;
  if (!insertion) {
    const statement = statementRangeAt(document, cursorOffset);
    const statementText = document.slice(statement.from, statement.to);
    const select = /\bselect\b/i.exec(statementText);
    if (!select) return null;
    const statementStart = statement.from;
    let position = statementStart + select.index + select[0].length;
    while (document[position] === ' ' || document[position] === '\t') position += 1;
    const from = /\bfrom\b/i.exec(document.slice(position, statement.to));
    const listEnd = from ? position + from.index : statement.to;
    const list = document.slice(position, listEnd);
    const star = /^\s*\*\s*$/.test(list);
    insertion = {
      document,
      position: star ? position + list.indexOf('*') : position,
      mode: star ? 'empty' : /^\s*$/.test(list) ? 'empty' : 'before-existing',
      statementFrom: statement.from,
      statementTo: statement.to,
    };
    if (star) {
      const nextDocument = `${document.slice(0, insertion.position)}${column}${document.slice(insertion.position + 1)}`;
      return {
        document: nextDocument,
        insertion: {
          ...insertion,
          document: nextDocument,
          position: insertion.position + column.length,
          statementTo: insertion.statementTo + column.length - 1,
        },
      };
    }
  }

  const text = insertion.mode === 'empty'
    ? `${continued ? ', ' : ''}${column}`
    : `${column}, `;
  const nextDocument = `${document.slice(0, insertion.position)}${text}${document.slice(insertion.position)}`;
  return {
    document: nextDocument,
    insertion: {
      document: nextDocument,
      position: insertion.position + text.length,
      mode: insertion.mode,
      statementFrom: insertion.statementFrom,
      statementTo: insertion.statementTo + text.length,
    },
  };
}

function databaseNodeKey(database: string): string {
  return `database:${encodeURIComponent(database)}`;
}

function tableNodeKey(database: string, table: string): string {
  return `table:${encodeURIComponent(database)}:${encodeURIComponent(table)}`;
}

function errorText(error: unknown): string {
  if (error instanceof UiApiError) {
    let primary = error.message;
    let metadata = '';
    if (typeof error.details === 'string') {
      metadata = error.details;
    } else if (error.details && typeof error.details === 'object') {
      const details = error.details as { message?: unknown; sqlState?: unknown; vendorCode?: unknown };
      if (typeof details.message === 'string' && details.message) primary = details.message;
      const vendorCode = typeof details.vendorCode === 'string' || typeof details.vendorCode === 'number'
        ? String(details.vendorCode)
        : '';
      const fields = [
        typeof details.sqlState === 'string' && details.sqlState ? `SQLState ${details.sqlState}` : '',
        vendorCode ? `code ${vendorCode}` : '',
      ].filter(Boolean);
      metadata = fields.length > 0 ? `(${fields.join(', ')})` : '';
    }
    return `${primary}${metadata ? ` ${metadata}` : ''} [${error.code}]`;
  }
  return error instanceof Error ? error.message : 'The operation failed.';
}

function messageSortValue(message: LogMessage): number {
  if (message.createdAt) return message.createdAt;
  const match = /^(\d{1,2}):(\d{2}):(\d{2})/.exec(message.timestamp);
  return match ? (Number(match[1]) * 3600 + Number(match[2]) * 60 + Number(match[3])) * 1000 : 0;
}

function sortMessages(messages: LogMessage[]): LogMessage[] {
  return [...messages].sort((left, right) => messageSortValue(right) - messageSortValue(left) || right.id - left.id);
}

function displayValue(value: unknown): string {
  if (value === null || value === undefined) return 'NULL';
  if (typeof value === 'string') return value;
  if (typeof value === 'number' || typeof value === 'boolean' || typeof value === 'bigint') return `${value}`;
  if (typeof value === 'symbol') return value.description ?? '';
  return JSON.stringify(value) ?? '';
}

function resultColumns(result: WebSqlExecutionResult): ColumnsType<Record<string, unknown>> {
  return result.columns.map((column, index) => ({
    title: <Tooltip title={column.type}>{column.name}</Tooltip>,
    dataIndex: `column-${index}`,
    key: `${column.name}-${index}`,
    ellipsis: true,
    render: (value: unknown) => <span className={value === null ? 'sql-null' : undefined}>{displayValue(value)}</span>,
  }));
}

function resultRows(result: WebSqlExecutionResult): Record<string, unknown>[] {
  return result.rows.map((row, rowIndex) => {
    const record: Record<string, unknown> = { key: rowIndex };
    row.forEach((value, columnIndex) => {
      record[`column-${columnIndex}`] = value;
    });
    return record;
  });
}

export function PlaygroundPage() {
  const session = useWebSqlSession();
  const executeSession = session.execute;
  const editorRef = useRef<EditorView | null>(null);
  const selectionRef = useRef<SqlSelection>({ from: 0, to: 0 });
  const schemaInsertionRef = useRef<SchemaInsertion | null>(null);
  const messageIdRef = useRef(0);
  const resultIdRef = useRef(0);
  const [messageApi, messageContext] = message.useMessage();
  const [editorValue, setEditorValue] = useState(INITIAL_SQL);
  const [running, setRunning] = useState(false);
  const [resetting, setResetting] = useState(false);
  const [closing, setClosing] = useState(false);
  const [results, setResults] = useState<QueryResultTab[]>([]);
  const [messages, setMessages] = useState<LogMessage[]>([]);
  const [historyLoadedSession, setHistoryLoadedSession] = useState<string | null>(null);
  const [activeTab, setActiveTab] = useState('messages');
  const [catalogs, setCatalogs] = useState<CatalogItem[]>([]);
  const [databases, setDatabases] = useState<string[]>([]);
  const [tablesByDatabase, setTablesByDatabase] = useState<Record<string, string[]>>({});
  const [loadedDatabases, setLoadedDatabases] = useState<Set<string>>(() => new Set());
  const [loadingDatabases, setLoadingDatabases] = useState<Set<string>>(() => new Set());
  const [expandedKeys, setExpandedKeys] = useState<Key[]>([]);
  const [metadataSearch, setMetadataSearch] = useState('');
  const [schema, setSchema] = useState<SchemaColumn[]>([]);
  const [catalog, setCatalog] = useState<string>();
  const [database, setDatabase] = useState<string>();
  const [table, setTable] = useState<string>();
  const [queryTableLimit, setQueryTableLimit] = useState(DEFAULT_QUERY_TABLE_LIMIT);
  const [metadataRequestCount, setMetadataRequestCount] = useState(0);
  const [metadataError, setMetadataError] = useState<string>();
  const [metadataWidth, setMetadataWidth] = useState(320);
  const [resizingMetadata, setResizingMetadata] = useState(false);
  const workspaceRef = useRef<HTMLElement | null>(null);
  const [editorHeight, setEditorHeight] = useState(330);
  const [resizingEditor, setResizingEditor] = useState(false);
  const workbenchRef = useRef<HTMLDivElement | null>(null);
  const metadataGenerationRef = useRef(0);
  const schemaRequestRef = useRef(0);
  const starRangeRef = useRef<{ document: string; from: number; to: number } | null>(null);
  const loadedDatabasesRef = useRef<Set<string>>(new Set());
  const tableLoadsRef = useRef<Map<string, Promise<void>>>(new Map());
  const catalogRef = useRef<string | undefined>(undefined);
  const metadataLoading = metadataRequestCount > 0;

  useEffect(() => {
    if (!resizingMetadata) return undefined;
    const handleMove = (event: PointerEvent) => {
      const workspace = workspaceRef.current;
      if (!workspace) return;
      const bounds = workspace.getBoundingClientRect();
      setMetadataWidth(Math.max(240, Math.min(520, event.clientX - bounds.left)));
    };
    const stopResize = () => setResizingMetadata(false);
    window.addEventListener('pointermove', handleMove);
    window.addEventListener('pointerup', stopResize);
    return () => {
      window.removeEventListener('pointermove', handleMove);
      window.removeEventListener('pointerup', stopResize);
    };
  }, [resizingMetadata]);

  useEffect(() => {
    if (!resizingEditor) return undefined;
    const handleMove = (event: PointerEvent) => {
      const workbench = workbenchRef.current;
      if (!workbench) return;
      const bounds = workbench.getBoundingClientRect();
      setEditorHeight(Math.max(180, Math.min(700, event.clientY - bounds.top - 65)));
    };
    const stopResize = () => setResizingEditor(false);
    window.addEventListener('pointermove', handleMove);
    window.addEventListener('pointerup', stopResize);
    return () => {
      window.removeEventListener('pointermove', handleMove);
      window.removeEventListener('pointerup', stopResize);
    };
  }, [resizingEditor]);

  const appendMessage = useCallback((tone: LogMessage['tone'], text: string) => {
    messageIdRef.current += 1;
    const createdAt = Date.now();
    setMessages((current) => [
      { id: messageIdRef.current, tone, timestamp: new Date(createdAt).toLocaleTimeString(), createdAt, text },
      ...current,
    ].sort((left, right) => messageSortValue(right) - messageSortValue(left) || right.id - left.id).slice(0, 100));
  }, []);

  useEffect(() => {
    const sessionId = session.sessionId;
    if (!sessionId || historyLoadedSession === sessionId) return;
    const history = loadHistory(sessionId);
    if (history) {
      if (results.length === 0) setResults(history.results as QueryResultTab[]);
      if (messages.length === 0) setMessages(sortMessages(history.messages as LogMessage[]));
      if (editorValue === INITIAL_SQL && history.editorValue) setEditorValue(history.editorValue);
      resultIdRef.current = history.results.length;
    }
    setHistoryLoadedSession(sessionId);
  }, [editorValue, historyLoadedSession, messages.length, results.length, session.sessionId]);

  useEffect(() => {
    if (!session.sessionId || historyLoadedSession !== session.sessionId) return;
    saveHistory(session.sessionId, { results, messages, editorValue });
  }, [editorValue, historyLoadedSession, messages, results, session.sessionId]);

  const executeMetadata = useCallback(async (statement: string) => {
    setMetadataRequestCount((count) => count + 1);
    try {
      return await executeSession(statement);
    } finally {
      setMetadataRequestCount((count) => Math.max(0, count - 1));
    }
  }, [executeSession]);

  const clearExplorerCache = useCallback(() => {
    metadataGenerationRef.current += 1;
    schemaRequestRef.current += 1;
    loadedDatabasesRef.current = new Set();
    tableLoadsRef.current.clear();
    setDatabases([]);
    setTablesByDatabase({});
    setLoadedDatabases(new Set());
    setLoadingDatabases(new Set());
    setExpandedKeys([]);
    setDatabase(undefined);
    setTable(undefined);
    setSchema([]);
  }, []);

  const loadDatabases = useCallback(async (selectedCatalog: string) => {
    clearExplorerCache();
    const generation = metadataGenerationRef.current;
    setMetadataError(undefined);
    try {
      const result = await executeMetadata(`SHOW DATABASES FROM ${quoteIdentifier(selectedCatalog)}`);
      if (generation === metadataGenerationRef.current && catalogRef.current === selectedCatalog) {
        setDatabases(adaptSingleNameColumn(result));
      }
    } catch (cause) {
      if (generation === metadataGenerationRef.current && catalogRef.current === selectedCatalog) {
        setMetadataError(errorText(cause));
      }
    }
  }, [clearExplorerCache, executeMetadata]);

  useEffect(() => {
    if (session.status !== 'ready' || catalogs.length > 0) return;
    let cancelled = false;
    void executeMetadata('SHOW CATALOGS').then((result) => {
      if (cancelled) return;
      const nextCatalogs = adaptCatalogs(result);
      setCatalogs(nextCatalogs);
      const initial = nextCatalogs.find((item) => item.current)?.name
        ?? nextCatalogs.find((item) => item.name === 'internal')?.name
        ?? nextCatalogs[0]?.name;
      if (initial) setCatalog(initial);
    }).catch(() => undefined);
    return () => { cancelled = true; };
  }, [catalogs.length, executeMetadata, session.status]);

  useEffect(() => {
    if (!catalog || session.status !== 'ready') return;
    catalogRef.current = catalog;
    void loadDatabases(catalog);
  }, [catalog, loadDatabases, session.status]);

  const loadTables = useCallback((databaseName: string): Promise<void> => {
    if (!catalog || session.status !== 'ready' || loadedDatabasesRef.current.has(databaseName)) {
      return Promise.resolve();
    }
    const existing = tableLoadsRef.current.get(databaseName);
    if (existing) return existing;

    const selectedCatalog = catalog;
    const generation = metadataGenerationRef.current;
    setMetadataError(undefined);
    setLoadingDatabases((current) => new Set(current).add(databaseName));
    const pending = executeMetadata(`SHOW TABLES FROM ${qualifiedName(selectedCatalog, databaseName)}`)
      .then((result) => {
        if (generation !== metadataGenerationRef.current || catalogRef.current !== selectedCatalog) return;
        const names = adaptSingleNameColumn(result);
        setTablesByDatabase((current) => ({ ...current, [databaseName]: names }));
        loadedDatabasesRef.current = new Set(loadedDatabasesRef.current).add(databaseName);
        setLoadedDatabases(new Set(loadedDatabasesRef.current));
      })
      .catch((cause) => {
        if (generation === metadataGenerationRef.current && catalogRef.current === selectedCatalog) {
          setMetadataError(errorText(cause));
        }
      })
      .finally(() => {
        if (tableLoadsRef.current.get(databaseName) === pending) tableLoadsRef.current.delete(databaseName);
        if (generation === metadataGenerationRef.current) {
          setLoadingDatabases((current) => {
            const next = new Set(current);
            next.delete(databaseName);
            return next;
          });
        }
      });
    tableLoadsRef.current.set(databaseName, pending);
    return pending;
  }, [catalog, executeMetadata, session.status]);

  const selectTable = useCallback(async (databaseName: string, tableName: string) => {
    if (!catalog || session.status !== 'ready') return;
    const selectedCatalog = catalog;
    const generation = metadataGenerationRef.current;
    const request = ++schemaRequestRef.current;
    setDatabase(databaseName);
    setTable(tableName);
    setSchema([]);
    setMetadataError(undefined);
    try {
      const result = await executeMetadata(`DESC ${qualifiedName(selectedCatalog, databaseName, tableName)}`);
      if (generation === metadataGenerationRef.current && request === schemaRequestRef.current
        && catalogRef.current === selectedCatalog) {
        setSchema(adaptSchema(result));
      }
    } catch (cause) {
      if (generation === metadataGenerationRef.current && request === schemaRequestRef.current) {
        setMetadataError(errorText(cause));
      }
    }
  }, [catalog, executeMetadata, session.status]);

  const treeData = useMemo<ExplorerTreeNode[]>(() => {
    const query = metadataSearch.trim().toLocaleLowerCase();
    return databases.flatMap((databaseName) => {
      const tables = tablesByDatabase[databaseName] ?? [];
      const databaseMatches = databaseName.toLocaleLowerCase().includes(query);
      const visibleTables = query && !databaseMatches
        ? tables.filter((tableName) => tableName.toLocaleLowerCase().includes(query))
        : tables;
      if (query && !databaseMatches && visibleTables.length === 0) return [];
      const loaded = loadedDatabases.has(databaseName);
      return [{
        key: databaseNodeKey(databaseName),
        title: databaseName,
        nodeType: 'database' as const,
        database: databaseName,
        isLeaf: loaded && tables.length === 0,
        children: loaded ? visibleTables.map((tableName) => ({
          key: tableNodeKey(databaseName, tableName),
          title: tableName,
          nodeType: 'table' as const,
          database: databaseName,
          table: tableName,
          isLeaf: true,
        })) : undefined,
      }];
    });
  }, [databases, loadedDatabases, metadataSearch, tablesByDatabase]);

  const run = useCallback(async () => {
    if (running || session.status !== 'ready') return;
    const selection = selectionRef.current;
    const statements = selection.from === selection.to
      ? sqlStatements(editorValue)
      : [executableSql(editorValue, selection)].filter(Boolean);
    if (statements.length === 0) {
      void messageApi.warning('Select or enter one SQL statement first.');
      return;
    }
    setRunning(true);
    let currentStatement = statements[0];
    try {
      let lastResultKey: string | null = null;
      for (const statement of statements) {
        currentStatement = statement;
        appendMessage('info', `Running: ${statement.replaceAll(/\s+/g, ' ').slice(0, 180)}`);
        const result = await session.execute(statement);
        resultIdRef.current += 1;
        const ordinal = resultIdRef.current;
        const key = `result-${Date.now()}-${ordinal}`;
        const tab = { key, label: `Result ${ordinal}`, sql: statement, result };
        setResults((current) => [tab, ...current].slice(0, MAX_RESULT_TABS));
        lastResultKey = key;
        appendMessage(
          'success',
          `Completed in ${result.elapsedTimeMs} ms · ${result.rows.length} returned row(s) · ${result.affectedRows} affected row(s)${result.truncated ? ' · result truncated' : ''}`,
        );
      }
      if (lastResultKey) setActiveTab(lastResultKey);
    } catch (cause) {
      appendMessage('error', `Failed: ${currentStatement.replaceAll(/\s+/g, ' ').slice(0, 180)} · ${errorText(cause)}`);
      setActiveTab('messages');
    } finally {
      setRunning(false);
    }
  }, [appendMessage, editorValue, messageApi, running, session]);

  const formatEditor = () => {
    const view = editorRef.current;
    if (!view) return;
    const current = view.state.selection.main;
    const source = current.empty ? view.state.doc.toString() : executableSql(view.state.doc.toString(), current);
    if (!source) return;
    try {
      const formatted = format(source, { language: 'mysql', keywordCase: 'upper' }).trim();
      const from = current.empty ? 0 : current.from;
      const to = current.empty ? view.state.doc.length : current.to;
      view.dispatch({ changes: { from, to, insert: formatted }, selection: { anchor: from + formatted.length } });
      view.focus();
    } catch (cause) {
      void messageApi.error(`Format failed: ${errorText(cause)}`);
    }
  };

  const insertText = (text: string) => {
    const view = editorRef.current;
    if (!view) {
      setEditorValue((current) => `${current}${text}`);
      return;
    }
    const selection = view.state.selection.main;
    view.dispatch({ changes: { from: selection.from, to: selection.to, insert: text }, selection: { anchor: selection.from + text.length } });
    view.focus();
  };

  const insertSchemaColumn = (columnName: string) => {
    const view = editorRef.current;
    const column = quoteIdentifier(columnName);
    if (!view) {
      setEditorValue((document) => {
        const result = insertSchemaColumnIntoSql(document, column, schemaInsertionRef.current, selectionRef.current.from);
        if (!result) return `${document}${column}`;
        schemaInsertionRef.current = result.insertion;
        return result.document;
      });
      return;
    }

    const document = view.state.doc.toString();
    const result = insertSchemaColumnIntoSql(document, column, schemaInsertionRef.current, selectionRef.current.from);
    if (!result) {
      insertText(column);
      schemaInsertionRef.current = null;
      return;
    }
    view.dispatch({
      changes: { from: 0, to: view.state.doc.length, insert: result.document },
      selection: { anchor: result.insertion.position },
    });
    schemaInsertionRef.current = result.insertion;
    view.focus();
  };

  const insertTableQuery = () => {
    if (!catalog || !database || !table) return;
    const template = `SELECT * FROM ${qualifiedName(catalog, database, table)} LIMIT ${queryTableLimit};`;
    schemaInsertionRef.current = null;
    const view = editorRef.current;
    if (!view) {
      setEditorValue((document) => {
        const separator = document.length === 0 ? '' : document.endsWith('\n\n') ? '' : document.endsWith('\n') ? '\n' : '\n\n';
        return `${document}${separator}${template}`;
      });
      return;
    }
    const document = view.state.doc.toString();
    const separator = document.length === 0 ? '' : document.endsWith('\n\n') ? '' : document.endsWith('\n') ? '\n' : '\n\n';
    const from = document.length + separator.length;
    const starFrom = from + 'SELECT '.length;
    const nextDocument = `${document}${separator}${template}`;
    starRangeRef.current = { document: nextDocument, from: starFrom, to: starFrom + 1 };
    view.dispatch({
      changes: { from: document.length, to: document.length, insert: `${separator}${template}` },
      selection: { anchor: starFrom, head: starFrom + 1 },
    });
    view.focus();
  };

  const reset = async () => {
    const oldSessionId = session.sessionId;
    if (oldSessionId) clearHistory(oldSessionId);
    setResetting(true);
    try {
      await session.reset();
      setResults([]);
      setHistoryLoadedSession(null);
      resultIdRef.current = 0;
      setMessages([]);
      setActiveTab('messages');
      clearExplorerCache();
      setCatalogs([]);
      setCatalog(undefined);
      catalogRef.current = undefined;
      appendMessage('success', 'The SQL connection was reset. Session variables and the current database were cleared.');
    } catch (cause) {
      appendMessage('error', `Reset failed: ${errorText(cause)}`);
    } finally {
      setResetting(false);
    }
  };

  const close = async () => {
    const oldSessionId = session.sessionId;
    if (oldSessionId) clearHistory(oldSessionId);
    setClosing(true);
    try {
      await session.close();
      clearExplorerCache();
      setResults([]);
      setMessages([]);
      setHistoryLoadedSession(null);
      setCatalogs([]);
      setCatalog(undefined);
      catalogRef.current = undefined;
      appendMessage('info', 'The SQL session was closed.');
    } catch (cause) {
      appendMessage('error', `Close failed: ${errorText(cause)}`);
    } finally {
      setClosing(false);
    }
  };

  const cancel = async () => {
    try {
      const response = await session.cancel();
      appendMessage('info', response.cancelRequested ? 'Cancel was requested for the running statement.' : 'There is no running statement to cancel.');
    } catch (cause) {
      appendMessage('error', `Cancel failed: ${errorText(cause)}`);
    }
  };

  const resultItems = useMemo(() => results.map((entry) => ({
    key: entry.key,
    label: entry.label,
    children: (
      <div className="query-result">
        <div className="result-summary">
          <span><strong>{entry.result.elapsedTimeMs}</strong> ms</span>
          <span><strong>{entry.result.rows.length}</strong> returned</span>
          <span><strong>{entry.result.affectedRows}</strong> affected</span>
          <span>Query ID <code>{entry.result.queryId || '—'}</code></span>
          {entry.result.truncated && <Tag color="warning">Truncated</Tag>}
        </div>
        {entry.result.warnings.length > 0 && (
          <Alert type="warning" showIcon title="Warnings" description={entry.result.warnings.join('\n')} />
        )}
        {entry.result.columns.length === 0 ? (
          <Empty image={Empty.PRESENTED_IMAGE_SIMPLE} description="The statement completed without a result set." />
        ) : (
          <Table
            className="result-table"
            size="small"
            scroll={{ x: 'max-content', y: 360 }}
            pagination={{ pageSize: 100, hideOnSinglePage: true }}
            columns={resultColumns(entry.result)}
            dataSource={resultRows(entry.result)}
          />
        )}
      </div>
    ),
  })), [results]);

  return (
    <main className="module-page playground-page">
      {messageContext}
      <header className="page-heading playground-heading">
        <div><h1>Playground</h1></div>
      </header>

      {session.status === 'error' && (
        <Alert type="error" showIcon title="The SQL session could not be created" description={errorText(session.error)} action={<Button onClick={() => void session.open()}>Retry</Button>} />
      )}
      {session.status === 'closed' && (
        <Alert type="info" showIcon title="The SQL session is closed" description="Open a new connection to continue." action={<Button type="primary" onClick={() => void session.open()}>Open session</Button>} />
      )}

      <section
        ref={workspaceRef}
        className={`playground-workspace${resizingMetadata ? ' is-resizing' : ''}`}
        style={{ gridTemplateColumns: `${metadataWidth}px 8px minmax(0, 1fr)` }}
      >
        <aside className="metadata-browser" aria-label="Metadata browser">
          <div className="panel-title"><h2>Metadata</h2>{metadataLoading && <Spin size="small" />}</div>
          {metadataError && <Alert type="error" showIcon title="Metadata unavailable" description={metadataError} />}
          <label>Catalog<Select aria-label="Catalog" value={catalog} loading={metadataLoading && catalogs.length === 0} options={catalogs.map((item) => ({ value: item.name, label: `${item.name} | ${item.type}` }))} onChange={setCatalog} placeholder="Select catalog" /></label>
          <div className="metadata-search">
            <Input
              aria-label="Search databases and loaded tables"
              allowClear
              value={metadataSearch}
              onChange={(event) => setMetadataSearch(event.target.value)}
              onPressEnter={() => { if (catalog) void loadDatabases(catalog); }}
              placeholder="Search databases / tables"
            />
            <Button aria-label="Refresh object explorer" title="Refresh object explorer" disabled={!catalog || session.status !== 'ready'} onClick={() => { if (catalog) void loadDatabases(catalog); }}>↻</Button>
          </div>
          <div className="metadata-tree" aria-label="Database and table tree">
            {databases.length === 0 && !metadataLoading ? (
              <Empty image={Empty.PRESENTED_IMAGE_SIMPLE} description={catalog ? 'No databases returned.' : 'Select a catalog.'} />
            ) : (
              <Tree<ExplorerTreeNode>
                blockNode
                expandedKeys={expandedKeys}
                loadedKeys={Array.from(loadedDatabases, databaseNodeKey)}
                selectedKeys={database && table ? [tableNodeKey(database, table)] : []}
                treeData={treeData}
                loadData={(node) => node.nodeType === 'database' ? loadTables(node.database) : Promise.resolve()}
                onExpand={(keys) => setExpandedKeys(keys)}
                onSelect={(_, info) => {
                  if (info.node.nodeType === 'database') {
                    setExpandedKeys((current) => current.includes(info.node.key) ? current : [...current, info.node.key]);
                    void loadTables(info.node.database);
                  } else if (info.node.table) {
                    void selectTable(info.node.database, info.node.table);
                  }
                }}
              />
            )}
            {loadingDatabases.size > 0 && <span className="metadata-tree-status">Loading tables…</span>}
          </div>
          <div className="schema-heading"><div><span>Table structure</span>{table && <strong>{table}</strong>}</div><Button size="small" disabled={!table} onClick={insertTableQuery}>Query table</Button></div>
          {schema.length === 0 ? <Empty image={Empty.PRESENTED_IMAGE_SIMPLE} description={table ? 'No columns returned.' : 'Select a table.'} /> : (
            <div className="schema-list">
              {schema.map((column) => (
                <button key={column.name} type="button" onClick={() => insertSchemaColumn(column.name)} title={`Insert ${column.name} into the editor`}>
                  <span>{column.name}</span><code>{column.type}</code>{column.key && <Tag>{column.key}</Tag>}
                </button>
              ))}
            </div>
          )}
        </aside>

        <div
          className="workspace-splitter"
          role="separator"
          aria-label="Resize metadata and SQL editor panels"
          aria-orientation="vertical"
          onPointerDown={(event) => {
            event.preventDefault();
            setResizingMetadata(true);
          }}
        />

        <div ref={workbenchRef} className={`sql-workbench${resizingEditor ? ' is-resizing' : ''}`}>
          <div className="editor-toolbar">
            <div><p className="ui-label">Statement editor</p></div>
            <div className={`connection-status session-${session.status}`}>
              <Tooltip title={`Session ID: ${session.sessionId ?? 'No session'}`}>
                <strong tabIndex={0}>{session.status}</strong>
              </Tooltip>
            </div>
            <Button size="small" onClick={formatEditor} disabled={running}>Format</Button>
            <Button size="small" danger onClick={() => void cancel()} disabled={!running}>Cancel</Button>
            <Button size="small" type="primary" onClick={() => void run()} loading={running} disabled={running || session.status !== 'ready'}>Run</Button>
          </div>
          <CodeMirror
            aria-label="SQL editor"
            className="sql-editor"
            value={editorValue}
            height={`${editorHeight}px`}
            extensions={[sql(), EditorView.lineWrapping]}
            onCreateEditor={(view) => { editorRef.current = view; }}
            onChange={(value, update) => {
              setEditorValue(value);
              if (starRangeRef.current?.document !== value) starRangeRef.current = null;
              selectionRef.current = update.state.selection.main;
            }}
            onUpdate={(update) => { selectionRef.current = update.state.selection.main; }}
            basicSetup={{ foldGutter: true, highlightActiveLine: true, highlightSelectionMatches: true }}
          />
          <div className="session-toolbar">
            <Select aria-label="Row limit for the generated query" title="Row limit used by Query table" size="small" className="query-limit-select" value={queryTableLimit} onChange={setQueryTableLimit} options={QUERY_TABLE_LIMITS.map((limit) => ({ value: limit, label: `LIMIT ${limit}` }))} />
            <Button size="small" onClick={() => void reset()} loading={resetting} disabled={running || session.status !== 'ready'}>Reset connection</Button>
            <Button size="small" danger onClick={() => void close()} loading={closing} disabled={running || session.status !== 'ready'}>Close session</Button>
          </div>
          <div
            className="workbench-splitter"
            role="separator"
            aria-label="Resize SQL editor and results panels"
            aria-orientation="horizontal"
            onPointerDown={(event) => {
              event.preventDefault();
              setResizingEditor(true);
            }}
          />
          <Tabs
            className="result-tabs"
            activeKey={activeTab}
            onChange={setActiveTab}
            type="editable-card"
            hideAdd
            onEdit={(targetKey, action) => {
              if (action !== 'remove' || targetKey === 'messages') return;
              setResults((current) => current.filter((entry) => entry.key !== targetKey));
              setActiveTab('messages');
            }}
            items={[
              ...resultItems,
              {
                key: 'messages',
                label: `Messages (${messages.length})`,
                closable: false,
                children: messages.length === 0 ? <Empty image={Empty.PRESENTED_IMAGE_SIMPLE} description="Execution messages will appear here." /> : (
                  <ol className="message-list">{messages.map((entry) => <li key={entry.id} className={`message-${entry.tone}`}><time>{entry.timestamp}</time><span>{entry.text}</span></li>)}</ol>
                ),
              },
            ]}
          />
        </div>
      </section>
    </main>
  );
}
