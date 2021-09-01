import { useState } from 'react';
import { PAGESIZE_OPTIONS, PAGINATION } from '@src/config';

function usePagination(config?: any) {
    const defaultConfig = config || PAGINATION;
    const [Pagination, setPagination] = useState(defaultConfig);
    const PaginationForView = {
        ...Pagination,
        showQuickJumper: false,
        size: 'small',
        showSizeChanger: true,
        pageSizeOptions: PAGESIZE_OPTIONS,
        total: 0,
    };
    const PaginationForViewWithoutCurrent = {};
    for (const [key, value] of Object.entries(PaginationForView)) {
        if (key !== 'current') {
            PaginationForViewWithoutCurrent[key] = value;
        }
    }

    return {
        Pagination,
        PaginationForView,
        PaginationForViewWithoutCurrent,
        setPagination,
    };
}

export { usePagination };
