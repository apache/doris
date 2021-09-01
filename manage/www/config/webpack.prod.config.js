const webpackBaseConfig = require("./webpack.base.config");
const { merge } = require("webpack-merge");
const path = require("path");
module.exports = merge(webpackBaseConfig, {});
