const devConfig = require("./config/webpack.dev.config");
const prodConfig = require("./config/webpack.prod.config");

let config = devConfig;
switch (process.env) {
  case "production":
    config = prodConfig;
    break;
  case "development":
    config = devConfig;
  default:
    config = devConfig;
}
module.exports = config;
