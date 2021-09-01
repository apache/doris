const webpackBaseConfig = require("./webpack.base.config");
const path = require("path");
const { merge } = require("webpack-merge");
const { webpack } = require("webpack");

const SERVER_QA = 'http://st01-meg-lijiu02-404.st01.baidu.com:8090';

const SERVER_RD = 'http://www.baidu.com:8080';
module.exports = merge(webpackBaseConfig, {
  mode: "development",
  devtool: "eval-source-map",
  // 开发服务配置
  devServer: {
    host: "localhost",
    port: 8080,
    publicPath: "/",
    disableHostCheck: true,
    useLocalIp: false,
    open: true,
    overlay: true,
    hot: true,
    stats: {
      assets: false,
      colors: true,
      modules: false,
      children: false,
      chunks: false,
      chunkModules: false,
      entrypoints: false,
    },
    proxy: {
      '/api': {
          target: SERVER_QA,
          changeOrigin: true,
          secure: false,
          // pathRewrite: {'^/commonApi': '/commonApi'}
      },
    },
    historyApiFallback: true,
  },
});
