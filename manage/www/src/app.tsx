import React from "react";
import { hot } from "react-hot-loader/root";
import { BrowserRouter as Router, Switch } from "react-router-dom";
import { RecoilRoot } from "recoil";
import routes from "./routes";
const App = () => {
  return <RecoilRoot>{routes}</RecoilRoot>;
};

export default hot(App);
