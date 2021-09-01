import { getDefaultPageSize } from './utils/utils';
import version from '../../version.json';

export const DEFAULT_NAMESPACE_ID = '0';

export const TABLE_DELAY = 150;
export const PAGESIZE_OPTIONS = ['10', '20', '50', '100', '200'];
export const PAGINATION = {
    current: 1,
    pageSize: getDefaultPageSize(),
    // total: 0
};
export const VERSION = `v${version.version}`;
