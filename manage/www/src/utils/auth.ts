class Auth {

    // token = localStorage.getItem('token');

    checkLogin(): boolean {
        const user: any = JSON.parse(localStorage.getItem('user') as string);
        if (user && user.name) {
            return true;
        }
        return true;
    }
}

export const auth = new Auth();