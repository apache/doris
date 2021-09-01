import CSSModules from 'react-css-modules';
import React, { useCallback, useState } from 'react';
import styles from './message-item.less';
import { messItemProps } from './message-item.interface';

function MessageItem(props: messItemProps) {
    return (
        <div styleName="mess-item">
            <div styleName="mess-item-title">
                <p styleName="mess-item-icon">{props.icon}</p>
                <p styleName="mess-item-name">{props.title}</p>
            </div>
            <div styleName="mess-item-content">
                <span>{props.des}</span>
            </div>
        </div>
    );
}

export const DashboardItem = CSSModules(styles)(MessageItem);
