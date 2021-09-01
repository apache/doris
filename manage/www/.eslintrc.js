/** @format */

module.exports = {
    parser: '@typescript-eslint/parser',
    extends: ['prettier/@typescript-eslint', 'plugin:prettier/recommended', 'plugin:@typescript-eslint/recommended'],
    parserOptions: {
        ecmaVersion: 2019,
        sourceType: 'module',
    },
    env: {
        browser: true,
        node: true,
    },
    rules: {
        'no-unused-vars': [1, { vars: 'all', args: 'after-used', ignoreRestSiblings: false }],
    },
};
