import Swal from 'sweetalert2';
import withReactContent from 'sweetalert2-react-content';

const DorisSwal = withReactContent(Swal);

class DorisModal {
    message(message) {
        DorisSwal.fire(message);
    }

    success(title, message = '') {
        return Swal.fire(title, message, 'success');
    }

    error(title = '', message = '') {
        return Swal.fire(title, message, 'error');
    }

    confirm(title, message = '', callback) {
        Swal.fire({
            title: title,
            text: message,
            icon: 'warning',
            showCancelButton: true,
            confirmButtonColor: '#3085d6',
            cancelButtonColor: '#d33',
            confirmButtonText: '确定',
            cancelButtonText: '取消',
        }).then(async result => {
            if (result.isConfirmed && typeof callback === 'function') {
                callback();
            } else if (!result.isConfirmed) {
                Swal.close();
            } else {
                this.success('操作成功');
            }
        });
    }
}

export const modal = new DorisModal();
