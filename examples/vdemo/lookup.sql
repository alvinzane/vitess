create table name_user_idx(name varchar(128), id int, primary key(name, id));
create view user as select * from name_user_idx;
create table rates(currency varchar(10), rate bigint, primary key(currency));
