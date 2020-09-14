/**
 * @file test cron
 * @author lpx
 * @since 2020/08/19
 */
module.exports = {
    plugins: {
    // 兼容浏览器，添加前缀
        'autoprefixer': {
            overrideBrowserslist: [
                'Chrome > 31',
                'ff > 31',
                'ie >= 8'
            ],
            grid: true
        }
    }
};