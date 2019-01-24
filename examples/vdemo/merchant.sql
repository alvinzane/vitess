create table merchant(name varchar(128), category varchar(128), primary key(name));
create table morder(id int, uid int, mname varchar(128), pid int, price int, primary key(id));
