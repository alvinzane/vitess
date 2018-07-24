create table user(id int, name varchar(128), balance bigint, primary key(id));
create table uorder(id int, uid int, mname varchar(128), pid int, primary key(id));
create table uproduct(id int, description varchar(128), primary key(id));
