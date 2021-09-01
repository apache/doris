import moment from 'moment';
import { message } from 'antd';
import { TableAPI } from '@src/routes/table-content/table.api';

export const getDefaultPageSize = () => JSON.parse(localStorage.getItem('pageSize') || '50');
export const deepClone = (data: any) => JSON.parse(JSON.stringify(data));

export function getShowTime(time: string) {
    if (time) {
        const res = moment(time, moment.ISO_8601).local().format('YYYY-MM-DD HH:mm:ss');
        return res;
    }
}
function getIdFromUrl(href: any) {
    if (href.indexOf('meta/table/') >= 0) {
        return href.split('meta/table/')[1].split('/')[0];
    } else if (href.indexOf('local-import/') >= 0) {
        return href.split('local-import/')[1];
    } else if (href.indexOf('system-import/') >= 0) {
        return href.split('system-import/')[1];
    } else {
        return '';
    }
}

export function formatBytes(bytes: number, decimals = 2, showSize = true) {
    if (bytes === 0) return '0 Bytes';
    const k = 1024,
        dm = decimals,
        sizes = ['Bytes', 'KB', 'MB', 'GB', 'TB', 'PB', 'EB', 'ZB', 'YB'],
        i = Math.floor(Math.log(bytes) / Math.log(k));
    return parseFloat((bytes / Math.pow(k, i)).toFixed(dm)) + (showSize ? ' ' + sizes[i] : '');
}

export function isTableIdSame() {
    const tableId = getIdFromUrl(window.location.href);
    const local_table_id = localStorage.getItem('table_id');
    if (tableId && local_table_id !== tableId) {
        TableAPI.getTableInfo({ tableId: tableId }).then(res => {
            const { msg, code, data } = res;
            if (code === 0) {
                localStorage.setItem('database_id', data.dbId);
                localStorage.setItem('database_name', data.dbName);
                localStorage.setItem('table_id', tableId);
                localStorage.setItem('table_name', data.name);
                window.location.reload();
            } else {
                message.error(msg);
            }
        });
    }
}