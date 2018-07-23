create table user(id int, name varchar(128), currency varchar(10), amount bigint, primary key(id));
create table urates(currency varchar(10), rate bigint, primary key(currency));
