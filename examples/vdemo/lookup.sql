create table name_user_idx(name varchar(128), uid int, primary key(name, uid));
create table product(id int, description varchar(128), primary key(id));
create table sales(pid int, amount int, primary key(pid));
