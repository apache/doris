/** @format */
import { DataNode } from './tree.interface';

export function updateTreeData(list: DataNode[], key: any, children: any) {
    return list.map(node => {
        if (node.key === key) {
            return {
                ...node,
                children,
            };
        }
        return node;
    });
}
