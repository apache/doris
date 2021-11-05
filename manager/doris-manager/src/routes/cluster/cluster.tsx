import { match } from "assert";
import React from "react";
import { Switch, Route, Redirect, useRouteMatch } from "react-router";
import { Overview } from "../dashboard/overview/overview";
import { ClusterMonitor } from "./monitor/monitor";

export function Cluster(props: any) {
    const match = useRouteMatch();
    return (
        <Switch>
            {/* <Route path={`${match.path}/list`} component={Overview} /> */}
            <Route path={`${match.path}/monitor`} component={ClusterMonitor} />
            <Redirect to={`${match.path}/monitor`} />
        </Switch>
    )
}