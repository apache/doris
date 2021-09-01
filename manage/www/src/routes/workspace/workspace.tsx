import React from "react";
import { Switch, Route, Redirect } from "react-router-dom";
import { Editor } from "./components/editor/editor";
import { SQLWorkspaceProps } from "./workspace.interface";

function Workspace(props: SQLWorkspaceProps) {
  const { match } = props;
  return (
    <>
      <Switch>
        <Route path={`${match.path}/editor`} component={Editor} />
        <Redirect to={`${match.path}/editor`} />
      </Switch>
    </>
  );
}

export default Workspace;
