import React, { useState, useEffect, useRef } from 'react';
import { Modal, Form, Input, message, Button } from 'antd';
import { TreeAPI } from '../tree.api';
import EventEmitter from 'src/utils/event-emitter';
const DatabaseModal = (props: any) => {
    const modalRef: any = useRef();
    const inputName: any = useRef();
    const inputDes: any = useRef();
    const [isModalVisible, setIsModalVisible] = useState(false);
    const [formData, setFormData] = useState({
        describe: '',
        name: '',
    });
    useEffect(() => {
        setIsModalVisible(props.isShow);
    }, [props.isShow]);
    const showModal = () => {
        setIsModalVisible(true);
    };

    const handleOk = () => {
        modalRef.current.validateFields().then((res: any) => {
            TreeAPI.newDatabase(formData).then((res: any) => {
                const { msg, code, data } = res;
                if (code === 0) {
                    message.success('创建成功！');
                    // setFormData({ describe: '', name: '' });
                    props.changeShow(false);
                    EventEmitter.emit('refreshTreeData');
                } else {
                    message.error(msg);
                }
            });
        });

        // setIsModalVisible(false);
    };

    const handleCancel = () => {
        // setFormData({ describe: '', name: '' });
        setTimeout(() => {
            props.changeShow(false);
        }, 200);
    };
    function handleChange(value: any, key: string) {
        const item = value.target.value;
        const localData = JSON.parse(JSON.stringify(formData));
        localData[key] = item;
        setFormData(localData);
    }
    return (
        <>
            <Modal
                title="创建新的数据库"
                visible={isModalVisible}
                footer={[
                    <Button key="submit" type="primary" onClick={handleOk}>
                        提交创建
                    </Button>,
                ]}
                onOk={handleOk}
                onCancel={handleCancel}
            >
                <Form ref={modalRef} name="basic" initialValues={{ remember: true }} layout="vertical">
                    <Form.Item label="数据库名称" name="name" rules={[{ required: true, message: '请输入数据库名称' }]}>
                        <Input
                            placeholder="输入你的数据库名称"
                            value={formData.name}
                            ref={inputName}
                            onChange={value => handleChange(value, 'name')}
                        />
                    </Form.Item>
                    <Form.Item
                        label="描述信息"
                        name="describe"
                        rules={[{ required: false, message: '请输入数据库描述' }]}
                    >
                        <Input
                            placeholder="输入你数据库描述信息，非必填写项，但便于后期运维建议填写"
                            value={formData.describe}
                            ref={inputDes}
                            onChange={value => handleChange(value, 'describe')}
                        />
                    </Form.Item>
                </Form>
            </Modal>
        </>
    );
};
export default DatabaseModal;
