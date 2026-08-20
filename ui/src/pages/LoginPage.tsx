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

import { useMutation } from '@tanstack/react-query';
import { Alert, Button, Form, Input, Spin } from 'antd';
import { useState } from 'react';
import { Navigate, useLocation, useNavigate } from 'react-router-dom';

import dorisLogo from '../assets/doris-logo-horizontal-primary.svg';
import { login } from '../api/auth';
import { UiApiError } from '../api/client';
import { useMe } from '../api/me';
import { queryClient } from '../app/queryClient';

interface LoginValues {
  username: string;
  password?: string;
}

interface LoginLocationState {
  reason?: 'expired' | 'signed-out';
  from?: string;
}

export function LoginPage() {
  const navigate = useNavigate();
  const location = useLocation();
  const [submitted, setSubmitted] = useState(false);
  const state = (location.state ?? {}) as LoginLocationState;
  const me = useMe();
  const loginMutation = useMutation({
    mutationFn: ({ username, password }: LoginValues) => login(username.trim(), password ?? ''),
    onSuccess: (data, values) => {
      queryClient.setQueryData(['ui', 'me'], data);
      void navigate(state.from && state.from !== '/login' ? state.from : '/home', {
        replace: true,
        state: (values.password ?? '') === '' ? { emptyPassword: true } : undefined,
      });
    },
  });

  if (me.isSuccess && !submitted) return <Navigate to="/home" replace />;

  const errorMessage =
    loginMutation.error instanceof UiApiError
      ? loginMutation.error.message
      : loginMutation.isError
        ? 'Sign-in failed. Please try again.'
        : null;

  return (
    <main className="login-page">
      <header className="login-header">
        <img src={dorisLogo} alt="Apache Doris" />
        <span className="ui-label">FE Web Console</span>
      </header>
      <section className="login-main">
        <div className="login-introduction">
          <p className="ui-label">Real-time analytics infrastructure</p>
          <h1>One clear view of your Doris cluster.</h1>
          <p>Sign in with an existing Doris user to inspect the tools available to your account.</p>
        </div>
        <div className="login-panel">
          <p className="ui-label">Authentication</p>
          <h2>Sign in</h2>
          <p className="login-help">Use your Apache Doris username and password.</p>
          {state.reason === 'expired' && (
            <Alert type="warning" showIcon title="Your session expired. Sign in again to continue." />
          )}
          {state.reason === 'signed-out' && <Alert type="success" showIcon title="You have signed out." />}
          {me.isPending && (
            <div className="login-session-check"><Spin size="small" /> Checking existing session…</div>
          )}
          {errorMessage && <Alert type="error" showIcon title={errorMessage} />}
          <Form<LoginValues>
            layout="vertical"
            requiredMark={false}
            initialValues={{ username: '', password: '' }}
            onFinish={(values) => {
              setSubmitted(true);
              loginMutation.mutate(values);
            }}
            disabled={loginMutation.isPending || me.isPending}
          >
            <Form.Item
              label="Username"
              name="username"
              rules={[
                { required: true, whitespace: true, message: 'Enter your Doris username.' },
                { max: 256, message: 'Username is too long.' },
              ]}
            >
              <Input autoComplete="username" autoFocus />
            </Form.Item>
            <Form.Item label="Password" name="password">
              <Input.Password autoComplete="current-password" placeholder="May be empty" />
            </Form.Item>
            <p className="login-help">An empty password works only when that Doris user is configured without one.</p>
            <Button type="primary" htmlType="submit" block loading={loginMutation.isPending}>
              Sign in
            </Button>
          </Form>
          <p className="login-footnote">There is no separate Web UI account or registration flow.</p>
        </div>
      </section>
      <footer className="login-footer">Access follows Doris users and privileges.</footer>
    </main>
  );
}
