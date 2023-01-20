package org.apache.doris.mysql.privilege;

public class Password {
    private byte[] password;

    public byte[] getPassword() {
        return password;
    }

    public void setPassword(byte[] password) {
        this.password = password;
    }
}
