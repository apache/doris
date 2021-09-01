class EventEmitter {
    public listeners: any;
    constructor() {
        this.listeners = {};
    }

    on(type: string, cb: any) {
        let cbs = this.listeners[type];
        if (!cbs) {
            cbs = [];
        }
        cbs.push(cb);
        this.listeners[type] = cbs;
        return () => {
            this.remove(type, cb);
        };
    }

    emit(type: any, ...args: any) {
        const cbs = this.listeners[type];
        if (Array.isArray(cbs)) {
            for (let i = 0; i < cbs.length; i++) {
                const cb = cbs[i];
                if (typeof cb === 'function') {
                    cb(...args);
                }
            }
        }
    }

    remove(type: any, cb: any) {
        if (cb) {
            let cbs = this.listeners[type];
            cbs = cbs.filter((eMap: any) => eMap.cb !== cb);
            this.listeners[type] = cbs;
        } else {
            this.listeners[type] = null;
            delete this.listeners[type];
        }
    }
}

export default new EventEmitter();
