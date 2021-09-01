export enum STATUS_MAP {
    RUNNING = 'RUNNING',
    EOF = 'EOF',
    ERR = 'ERR',
}

export const FILTERS = [
    { text: STATUS_MAP.RUNNING, value: STATUS_MAP.RUNNING },
    { text: STATUS_MAP.EOF, value: STATUS_MAP.EOF },
    { text: STATUS_MAP.ERR, value: STATUS_MAP.ERR },
];
