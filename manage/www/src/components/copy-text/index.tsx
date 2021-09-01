import React from 'react';
import { CopyOutlined } from '@ant-design/icons';
import { message } from 'antd';
import './index.less';

export default function CopyText(props: any) {
    function handleCopy() {
        const input = document.createElement('input');
        input.style.opacity = '0';
        input.setAttribute('readonly', 'readonly');
        input.setAttribute('value', props.text);
        document.body.appendChild(input);
        input.setSelectionRange(0, 9999);
        input.select();
        if (document.execCommand('copy')) {
            document.execCommand('copy');
            message.success('复制成功');
        }
        document.body.removeChild(input);
    }
    return (
        <div className="copy-wrap">
            {props.children}
            <CopyOutlined className="copy-icon" onClick={handleCopy} />
        </div>
    );
}
