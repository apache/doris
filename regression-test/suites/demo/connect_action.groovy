def result1 = connect(user = 'admin', password = context.config.jdbcPassword, url = context.config.jdbcUrl) {
    // execute sql with admin user
    sql 'select 99 + 1'
}

// if not specify <user, password, url>, it will be set to context.config.jdbc<User, Password, Url>
//
// user: 'root'
// password: context.config.jdbcPassword
// url: context.config.jdbcUrl
def result2 = connect('root') {
    // execute sql with root user
    sql 'select 50 + 50'
}

assertEquals(result1, result2)