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
import { Button, Drawer, Layout, Menu, message } from 'antd';
import { useEffect, useState } from 'react';
import { Outlet, useLocation, useNavigate } from 'react-router-dom';

import dorisLogoDark from '../../design/assets/doris-logo-horizontal-dark.svg';
import { logout } from '../api/auth';
import { setCsrfToken } from '../api/csrf';
import type { UiMe } from '../api/types';
import { queryClient } from './queryClient';

interface NavigationItem {
  key: string;
  label: string;
  index: string;
}

const navigation: NavigationItem[] = [
  { key: '/home', label: 'Cluster Overview', index: '01' },
  { key: '/playground', label: 'Playground', index: '02' },
  { key: '/system', label: 'Proc System', index: '03' },
  { key: '/log', label: 'Log', index: '04' },
  { key: '/query-profiles', label: 'Query Profiles', index: '05' },
  { key: '/sessions', label: 'Sessions', index: '06' },
  { key: '/configuration', label: 'Configuration', index: '07' },
];

function selectedPath(pathname: string): string {
  return navigation.find((item) => pathname === item.key || pathname.startsWith(`${item.key}/`))?.key ?? '';
}

export function AppShell({ me }: { me: UiMe }) {
  const navigate = useNavigate();
  const location = useLocation();
  const [mobileOpen, setMobileOpen] = useState(false);
  const [messageApi, messageContext] = message.useMessage();

  const logoutMutation = useMutation({
    mutationFn: logout,
    onSuccess: () => {
      setCsrfToken(null);
      queryClient.clear();
      void navigate('/login', { replace: true, state: { reason: 'signed-out' } });
    },
    onError: () => {
      void messageApi.error('Sign out failed. Please try again.');
    },
  });

  useEffect(() => {
    const handleUnauthorized = () => {
      setCsrfToken(null);
      queryClient.clear();
      void navigate('/login', { replace: true, state: { reason: 'expired' } });
    };
    window.addEventListener('doris-ui:unauthorized', handleUnauthorized);
    return () => window.removeEventListener('doris-ui:unauthorized', handleUnauthorized);
  }, [navigate]);

  const menuItems = navigation.map((item) => ({
    key: item.key,
    label: (
      <span className="nav-label">
        <span>{item.index}</span>
        {item.label}
      </span>
    ),
  }));

  const onNavigate = ({ key }: { key: string }) => {
    setMobileOpen(false);
    void navigate(key);
  };

  const sidebar = (
    <div className="app-sidebar-inner">
      <div className="app-brand">
        <img src={dorisLogoDark} alt="Apache Doris" />
      </div>
      <Menu
        className="primary-navigation"
        theme="dark"
        mode="inline"
        selectedKeys={[selectedPath(location.pathname)]}
        items={menuItems}
        onClick={onNavigate}
      />
      <div className="sidebar-account">
        <span className="ui-label">Signed in as</span>
        <strong>{me.user}</strong>
        <Button
          type="text"
          className="sidebar-sign-out"
          loading={logoutMutation.isPending}
          onClick={() => logoutMutation.mutate()}
        >
          Sign out
        </Button>
      </div>
    </div>
  );

  return (
    <Layout className="app-layout">
      {messageContext}
      <Layout.Sider width={240} className="desktop-sidebar">
        {sidebar}
      </Layout.Sider>
      <Drawer
        className="mobile-navigation-drawer"
        placement="left"
        size="default"
        open={mobileOpen}
        onClose={() => setMobileOpen(false)}
        styles={{ body: { padding: 0 } }}
        title={null}
      >
        {sidebar}
      </Drawer>
      <Layout className="app-workspace">
        <Layout.Header className="app-utility-bar">
          <Button className="mobile-menu-button" onClick={() => setMobileOpen(true)} aria-label="Open navigation">
            Menu
          </Button>
          <div className="breadcrumb-text">
            Console / <strong>{navigation.find((item) => item.key === selectedPath(location.pathname))?.label ?? 'Page'}</strong>
          </div>
          <span className="fe-connection"><i aria-hidden="true" />FE connected</span>
          <span className="utility-user">{me.user}</span>
        </Layout.Header>
        <Layout.Content className="app-content">
          <Outlet context={me} />
        </Layout.Content>
      </Layout>
    </Layout>
  );
}
