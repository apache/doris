import React, { PropsWithChildren } from 'react';
import { SpinProps } from 'antd/lib/spin';
import { Spin } from 'antd';
import { TABLE_DELAY } from '@src/config';

type SpinWithoutSpinningProp = Omit<SpinProps, 'spinning'>;

interface LoadingWrapperProps extends SpinWithoutSpinningProp {
    loading?: boolean;
}

export function LoadingWrapper(props: PropsWithChildren<LoadingWrapperProps>) {
    let loading = false;
    if (props.loading) {
        loading = true;
    }
    const spinProps = { ...props };
    delete spinProps.loading;
    return (
        <Spin spinning={loading} delay={TABLE_DELAY} {...spinProps}>
            {props.children}
        </Spin>
    );
}
