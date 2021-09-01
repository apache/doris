import { Tooltip } from 'antd';
import React, { useRef, useEffect, useState } from 'react';
import CopyText from '../copy-text';

export function ClippedText(props: any) {
    const refClip = useRef<HTMLDivElement>(null);
    const [isShowTip, setShowTip] = useState<boolean>(false);
    let tempWidth = props.width;
    if (props.inTable && typeof props.width === 'number') {
        tempWidth = props.width - 40;
    }
    let width;
    if (props.width) {
        width = typeof props.width === 'number' ? `${tempWidth}px` : props.width;
    } else {
        width = 'calc(100% - 10px)';
    }
    const title = props.text ? props.text : props.children;

    useEffect(() => {
        const elem = refClip.current;
        if (elem) {
            if (elem.scrollWidth > elem.clientWidth) {
                setShowTip(true);
            }
        }
    }, []);

    const content = (
        <div
            className="clipped-text"
            ref={refClip}
            style={{
                // width,
                maxWidth: width,
                overflow: 'hidden',
                whiteSpace: 'nowrap',
                textOverflow: 'ellipsis',
                ...props.style,
            }}
        >
            {props.children}
        </div>
    );

    return isShowTip ? (
        <CopyText text={title}>
            <Tooltip placement="top" title={title} overlayStyle={{ maxHeight: 400, overflow: 'auto', maxWidth: 600 }}>
                {content}
            </Tooltip>
        </CopyText>
    ) : (
        content
    );
}
