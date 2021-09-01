import { http } from '@src/utils/http';

function getSchema(tableId: string) {
    return http.get(`/api/meta/tableId/${tableId}/schema`);
}

export const SchemaAPI = {
    getSchema,
};
