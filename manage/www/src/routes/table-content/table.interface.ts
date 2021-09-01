/** @format */

export interface GetTableInfoRequestParams {
    tableId: string;
}
export interface TableInfoResponse {
    createTime: string;
    creator: string;
    describe: string;
    name: string;
    updateTime: string;
    dbId: string;
    dbName: string;
}
export interface sqlRequestParams {
    dbId: string | null;
    nsId: string;
    data: any;
}
