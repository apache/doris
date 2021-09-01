/** @format */

import React, { useState, useCallback, useEffect } from 'react';
import styles from './header.module.less';
import { HeaderProps } from './header.interface';
import { SyncOutlined } from '@ant-design/icons';
import { HeaderAPI } from './header.api';
import CSSModules from 'react-css-modules';
const EventEmitter = require('events').EventEmitter; 
const event = new EventEmitter();

export function Header(props: HeaderProps) {
    const [loading, setLoading] = useState(false);
    // useEffect(() => {
    //     HeaderAPI.refreshData();
    // }, []);
    function refresh() {
        HeaderAPI.refreshData();
        event.emit('refreshData');
        props.callback();
        setTimeout(() => {
            setLoading(false);
        }, 300);
    }
    return (
        <div styleName="common-header">
            <div styleName="common-header-title">
                <span styleName="common-header-icon">{props.icon}</span>
                <span styleName="common-header-name">{props.title}</span>
            </div>
            <div styleName="common-header-refresh">
                <SyncOutlined
                    spin={loading}
                    onClick={() => {
                        refresh();
                        setLoading(true);
                    }}
                />
            </div>
        </div>
    );
}

export const CommonHeader = CSSModules(styles)(Header);