/**
 * @file test cron
 * @author lpx
 * @since 2020/08/19
 */
const devConfig = require('./config/webpack.dev');
const prodConfig = require('./config/webpack.prod');

const SpeedMeasurePlugin = require('speed-measure-webpack-plugin');

const smp = new SpeedMeasurePlugin();

let config = devConfig;

switch (process.env.NODE_ENV) {
    case 'prod':
    case 'production':
        config = prodConfig;
        break;
    case 'dev':
    case 'development':
        config = devConfig;
        break;
    default:
        config = devConfig;
}

const webpackConfig = config;

module.exports = smp.wrap(webpackConfig);
