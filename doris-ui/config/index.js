'use strict'

const path = require('path')

module.exports = {
    dev: {
        assetsSubDirectory: 'static',
        assetsPublicPath: '/',
        proxyTable: {
            '/api': {
                target: 'http://192.168.1.130:8080', 
                changeOrigin: true, 
                pathRewrite: { 
                    '^/api': '/api'
                }
            }
        },

        host: 'localhost', 
        port: 9999, 
        autoOpenBrowser: true,
        errorOverlay: true,
        notifyOnErrors: true,
        poll: false, 
        devtool: 'cheap-module-eval-source-map',
        cacheBusting: true,
        cssSourceMap: true
    },

    build: {
        index: path.resolve(__dirname, '../dist/index.html'),
        assetsRoot: path.resolve(__dirname, '../dist'),
        assetsSubDirectory: 'static',
        assetsPublicPath: './',
        productionSourceMap: true,
        devtool: '#source-map',
        productionGzip: false,
        productionGzipExtensions: ['js', 'css'],
        bundleAnalyzerReport: process.env.npm_config_report
    }
}