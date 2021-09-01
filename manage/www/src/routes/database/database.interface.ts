/** @format */

export interface GetDatabaseInfoRequestParams {
    dbId: string;
}
export interface DatabaseInfoResponse {
    createTime: string;
    creator: string;
    describe: string;
    name: string;
}
