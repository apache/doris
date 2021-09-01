import { RouteComponentProps } from 'react-router';

export interface DashboardProps extends RouteComponentProps<any> {
    
}
export interface GetMetaInfoRequestParams {
    nsId: string;
}
export interface MetaInfoResponse {
    beCount: number;
    dbCount: number;
    diskOccupancy: number;
    feCount: number;
    remainDisk: number;
    tblCount: number;
}
