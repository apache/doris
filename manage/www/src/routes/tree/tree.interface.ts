/** @format */

export interface GetDatabaseRequestParams {
    nsId: string;
}

export interface GetDatabaseInfoByDbIdRequestParams {
    dbId: string;
}
export interface GetTablesByDbIdRequestParams {
    dbId: string;
}
export interface ResNode {
    id: number;
    name: string;
}
export interface NewDatabaseRequestParams {
    describe: string;
    name: string;
}

export interface DataNode {
    title: string;
    key: string;
    isLeaf?: boolean;
    children?: DataNode[];
    icon?: any;
}
