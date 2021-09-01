import * as React from "react";
import { Suspense } from "react";
import {
  Route,
  Switch,
  Redirect,
  BrowserRouter as Router,
} from "react-router-dom";
import { Loading } from "./components/loading";
import { NotFound } from "./components/not-found";
import Container from "./routes/container";
import { auth } from "./utils/auth";
// 对状态属性进行监听
const routes = (
  <Suspense fallback={<Loading />}>
    <Router>
      <Switch>
        <Route
          path="/"
          render={(props) =>
            auth.checkLogin() ? (
              <Container {...props} />
            ) : props.history.location.pathname === "/login" ? (
              // <Login {...props} />
              <></>
            ) : (
              <Redirect to="/login" />
            )
          }
        />
        <Route
          path="/login"
          render={(props) =>
            auth.checkLogin() ? (
              <Redirect to="/" />
            ) : (
              <></>
              // <Login {...props} />
            )
          }
        />
        <Route component={NotFound} />
      </Switch>
    </Router>
  </Suspense>
);

export default routes;
