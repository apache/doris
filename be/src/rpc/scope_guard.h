// Copyright (c) 2017, Baidu.com, Inc. All Rights Reserved

// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.

#ifndef BDG_PALO_BE_SRC_RPC_SCOPE_GUARD_H
#define BDG_PALO_BE_SRC_RPC_SCOPE_GUARD_H

namespace palo {

/** Base class for the ScopeGuards */
class ScopeGuardImplBase {
protected:
    ~ScopeGuardImplBase() { }

    ScopeGuardImplBase(const ScopeGuardImplBase& other) throw()
        : m_dismissed(other.m_dismissed) {
            other.dismiss();
        }

    template <typename GuardT>
        static void safe_run(GuardT& guard) throw() {
            if (!guard.m_dismissed) {
                try {
                    guard.run();
                } catch (...) {}
            }
        }

public:

    ScopeGuardImplBase() throw() : m_dismissed(false) { }
    void dismiss() const throw() { m_dismissed = true; }

private:

    ScopeGuardImplBase& operator=(const ScopeGuardImplBase&); // not assignable
    mutable bool m_dismissed;
};

/*
 * According to the C++ standard, a reference initialized with a temporary
 * value makes that temporary value live for the lifetime of the reference
 * itself. This can save us from having to explicitly instantiate the guards
 * with long type parameters
 */
typedef const ScopeGuardImplBase &ScopeGuard;

/**
 * ScopeGuard implementation for free function with no paramter
 */
template <typename FunT>
class ScopeGuardImpl0 : public ScopeGuardImplBase {
public:

    static ScopeGuardImpl0<FunT> make_guard(FunT fun) {
        return ScopeGuardImpl0<FunT>(fun);
    }

    ~ScopeGuardImpl0() throw() { safe_run(*this); }

    void run() { m_fun(); }

private:

    ScopeGuardImpl0(FunT fun) : m_fun(fun) { }
    FunT m_fun;
};

template <typename FunT>
inline ScopeGuardImpl0<FunT> make_guard(FunT fun) {
    return ScopeGuardImpl0<FunT>::make_guard(fun);
}

/**
 * ScopeGuard implementation for free function with 1 parameter
 */
template <typename FunT, typename P1T>
class ScopeGuardImpl1 : public ScopeGuardImplBase {
    public:
        static ScopeGuardImpl1<FunT, P1T> make_guard(FunT fun, P1T p1) {
            return ScopeGuardImpl1<FunT, P1T>(fun, p1);
        }
        ~ScopeGuardImpl1() throw() { safe_run(*this); }
        void run() { m_fun(m_p1); }
    private:
        ScopeGuardImpl1(FunT fun, P1T p1) : m_fun(fun), m_p1(p1) { }
        FunT m_fun;
        const P1T m_p1;
};

template <typename FunT, typename P1T>
inline ScopeGuardImpl1<FunT, P1T> make_guard(FunT fun, P1T p1) {
    return ScopeGuardImpl1<FunT, P1T>::make_guard(fun, p1);
}

/**
 * ScopeGuard implementation for free function with 2 parameters
 */
template <typename FunT, typename P1T, typename P2T>
class ScopeGuardImpl2: public ScopeGuardImplBase {
    public:
        static ScopeGuardImpl2<FunT, P1T, P2T> make_guard(FunT fun, P1T p1, P2T p2) {
            return ScopeGuardImpl2<FunT, P1T, P2T>(fun, p1, p2);
        }
        ~ScopeGuardImpl2() throw() { safe_run(*this); }
        void run() { m_fun(m_p1, m_p2); }
    private:
        ScopeGuardImpl2(FunT fun, P1T p1, P2T p2) : m_fun(fun), m_p1(p1), m_p2(p2) { }
        FunT m_fun;
        const P1T m_p1;
        const P2T m_p2;
};

template <typename FunT, typename P1T, typename P2T>
inline ScopeGuardImpl2<FunT, P1T, P2T> make_guard(FunT fun, P1T p1, P2T p2) {
    return ScopeGuardImpl2<FunT, P1T, P2T>::make_guard(fun, p1, p2);
}

/**
 * ScopeGuard implementation for free function with 3 parameters
 */
template <typename FunT, typename P1T, typename P2T, typename P3T>
class ScopeGuardImpl3 : public ScopeGuardImplBase {
    public:
        static ScopeGuardImpl3<FunT, P1T, P2T, P3T>
            make_guard(FunT fun, P1T p1, P2T p2, P3T p3) {
                return ScopeGuardImpl3<FunT, P1T, P2T, P3T>(fun, p1, p2, p3);
            }
        ~ScopeGuardImpl3() throw() { safe_run(*this); }
        void run() { m_fun(m_p1, m_p2, m_p3); }
    private:
        ScopeGuardImpl3(FunT fun, P1T p1, P2T p2, P3T p3)
            : m_fun(fun), m_p1(p1), m_p2(p2), m_p3(p3) { }
        FunT m_fun;
        const P1T m_p1;
        const P2T m_p2;
        const P3T m_p3;
};

template <typename FunT, typename P1T, typename P2T, typename P3T>
inline ScopeGuardImpl3<FunT, P1T, P2T, P3T>
make_guard(FunT fun, P1T p1, P2T p2, P3T p3) {
    return ScopeGuardImpl3<FunT, P1T, P2T, P3T>::make_guard(fun, p1, p2, p3);
}

/**
 * ScopeGuard implementation for method with no parameter
 */
template <class ObjT, typename MethodT>
class ObjScopeGuardImpl0 : public ScopeGuardImplBase {
    public:
        static ObjScopeGuardImpl0<ObjT, MethodT>
            make_obj_guard(ObjT &obj, MethodT method) {
                return ObjScopeGuardImpl0<ObjT, MethodT>(obj, method);
            }
        ~ObjScopeGuardImpl0() throw() { safe_run(*this); }
        void run() { (m_obj.*m_method)(); }
    private:
        ObjScopeGuardImpl0(ObjT &obj, MethodT method)
            : m_obj(obj), m_method(method) { }
        ObjT &m_obj;
        MethodT m_method;
};

template <class ObjT, typename MethodT>
inline ObjScopeGuardImpl0<ObjT, MethodT>
make_obj_guard(ObjT &obj, MethodT method) {
    return ObjScopeGuardImpl0<ObjT, MethodT>::make_obj_guard(obj, method);
}

/**
 * ScopeGuard implementation for method with 1 parameter
 */
template <class ObjT, typename MethodT, typename P1T>
class ObjScopeGuardImpl1 : public ScopeGuardImplBase {
    public:
        static ObjScopeGuardImpl1<ObjT, MethodT, P1T>
            make_obj_guard(ObjT &obj, MethodT method, P1T p1) {
                return ObjScopeGuardImpl1<ObjT, MethodT, P1T>(obj, method, p1);
            }
        ~ObjScopeGuardImpl1() throw() { safe_run(*this); }
        void run() { (m_obj.*m_method)(m_p1); }
    protected:
        ObjScopeGuardImpl1(ObjT &obj, MethodT method, P1T p1)
            : m_obj(obj), m_method(method), m_p1(p1) { }
        ObjT &m_obj;
        MethodT m_method;
        const P1T m_p1;
};

template <class ObjT, typename MethodT, typename P1T>
inline ObjScopeGuardImpl1<ObjT, MethodT, P1T>
make_obj_guard(ObjT &obj, MethodT method, P1T p1) {
    return ObjScopeGuardImpl1<ObjT, MethodT, P1T>::make_obj_guard(obj, method, p1);
}

/**
 * ScopeGuard implementation for method with 2 parameters
 */
template <class ObjT, typename MethodT, typename P1T, typename P2T>
class ObjScopeGuardImpl2 : public ScopeGuardImplBase {
    public:
        static ObjScopeGuardImpl2<ObjT, MethodT, P1T, P2T>
            make_obj_guard(ObjT &obj, MethodT method, P1T p1, P2T p2) {
                return ObjScopeGuardImpl2<ObjT, MethodT, P1T, P2T>(obj, method, p1, p2);
            }
        ~ObjScopeGuardImpl2() throw() { safe_run(*this); }
        void run() { (m_obj.*m_method)(m_p1, m_p2); }
    private:
        ObjScopeGuardImpl2(ObjT &obj, MethodT method, P1T p1, P2T p2)
            : m_obj(obj), m_method(method), m_p1(p1), m_p2(p2) { }
        ObjT &m_obj;
        MethodT m_method;
        const P1T m_p1;
        const P2T m_p2;
};

template <class ObjT, typename MethodT, typename P1T, typename P2T>
inline ObjScopeGuardImpl2<ObjT, MethodT, P1T, P2T>
make_obj_guard(ObjT &obj, MethodT method, P1T p1, P2T p2) {
    return ObjScopeGuardImpl2<ObjT, MethodT, P1T, P2T>::make_obj_guard(obj,
            method, p1, p2);
}

/**
 * Helper class used to pass a parameter to the ScopeGuard by reference.
 *
 * e.g.:
 *     inline void decr(int &x) { --x; }
 *
 *     void example() {
 *       int i = 0;
 *       ScopedGuard guard = make_guard(decr, by_ref(i));
 *       // ...
 *     }
 */
template <class T>
class RefHolder {
    public:
        RefHolder(T& ref) : m_ref(ref) {}
        operator T& () const { return m_ref; }
    private:
        RefHolder& operator=(const RefHolder&); // not assignable
        T& m_ref;
};

template <class T>
inline RefHolder<T> by_ref(T& t) {
    return RefHolder<T>(t);
}

} // namespace palo

// The two level macros are needed for var<lineno>.
// Otherwise, it'd be literally var__LINE__
#define HT_CONCAT_(s1, s2) s1##s2
#define HT_CONCAT(s1, s2) HT_CONCAT_(s1, s2)
#define HT_AUTO_VAR(var) HT_CONCAT(var, __LINE__)

#define HT_ON_SCOPE_EXIT(...) \
    ScopeGuard HT_AUTO_VAR(guard) = make_guard(__VA_ARGS__); \
HT_UNUSED(HT_AUTO_VAR(guard))

#define HT_ON_OBJ_SCOPE_EXIT(...) \
    ScopeGuard HT_AUTO_VAR(guard) = make_obj_guard(__VA_ARGS__); \
HT_UNUSED(HT_AUTO_VAR(guard))

#endif //BDG_PALO_BE_SRC_RPC_SCOPE_GUARD_H
