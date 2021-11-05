export interface UserInfo {
    email:          string;
    name:           string;
    authType:       string;
    id:             number;
    collectionId:   number;
    ldap_auth:      boolean;
    last_login:     Date;
    updated_at:     Date;
    group_ids:      number[];
    date_joined:    Date;
    common_name:    string;
    google_auth:    boolean;
    space_id:       number;
    space_complete: boolean;
    deploy_type:    string;
    manager_enable: boolean;
    is_active:      boolean;
    is_admin:       boolean;
    is_qbnewb:      boolean;
    is_super_admin: boolean;
}