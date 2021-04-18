/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 * 
 *   http://www.apache.org/licenses/LICENSE-2.0
 * 
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
 
/**
 * @file test cron
 * @author lpx
 * @since 2020/08/19
 */
import React from 'react';
import {BrowserRouter as Router, Switch} from 'react-router-dom';
// import {renderRoutes} from 'react-router-config';
import routes from './router';
import renderRoutes from './router/renderRouter';
import 'antd/dist/antd.css';
import {getBasePath} from 'Src/utils/utils';
let basePath = getBasePath();
function App() {
    return (
        <Router 
            basename={basePath}
        >
            <Switch>
                {renderRoutes(routes.routes)}
            </Switch>
        </Router>
    );
};

export default App;
 
