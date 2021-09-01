export interface IResult<T> {
    msg: string;
    code: number;
    data: T;
    count: number;
}
