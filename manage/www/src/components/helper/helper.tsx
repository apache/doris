import { QuestionCircleFilled } from '@ant-design/icons';
import { Tooltip } from 'antd';
import React from 'react';
import { TooltipProps } from 'antd/lib/tooltip';
import './helper.less';

export function Helper(props: TooltipProps) {
    return (
        <Tooltip {...props}>
            <QuestionCircleFilled style={{ cursor: 'pointer', ...props.style }} className="palo-studio-helper" />
        </Tooltip>
    );
}
