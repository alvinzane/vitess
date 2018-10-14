create table by_tenant(tenant_id int, user_id int, app varchar(128), mon int, spent int, rcount int, primary key(tenant_id, user_id, app, mon));
