/** @format */

export interface GetTableListRequestParams {
    tableId: string | null;
    page: number;
    pageSize: number;
}
export interface TableInfo {
    id: string | null;
    name: string | null;
}
export interface TableListResponse {
    createTime: string;
    creator: string;
    describe: string;
    name: string;
    updateTime: string;
}
