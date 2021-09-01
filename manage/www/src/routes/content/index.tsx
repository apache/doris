/**
 * @format
 */
import React, { useEffect, useState } from 'react';
import { renderRoutes } from 'react-router-config';
import CSSModules from 'react-css-modules';
import styles from './content.module.less';
import { PageSide } from '@src/layout/page-side/index';
import { MetaBaseTree } from '../tree/index';
import { Redirect, Route, Router, Switch } from 'react-router-dom';
import TableContent from '../table-content';
import Database from '../database';

function MiddleContent(props: any) {
    return (
        <div styleName="palo-new-main">
            <div styleName="new-main-sider">
                <PageSide>
                    <MetaBaseTree></MetaBaseTree>
                </PageSide>
            </div>
            <div
                styleName="site-layout-background new-main-content"
                style={{
                    margin: '15px',
                    marginTop: 0,
                    height: 'calc(100vh - 95px)',
                    // overflow: 'hidden'
                }}
            >
                <Switch>
                    <Route path="/:nsId/content/table/:tableId" component={TableContent}/>
                    <Route path="/:nsId/content/database" component={Database}/>
                </Switch>
            </div>
        </div>
    );
}

export default CSSModules(styles, { allowMultiple: true })(MiddleContent);
