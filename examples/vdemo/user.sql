create table user(id int, name varchar(128), balance bigint, primary key(id));
create table uorder(id int, user_id int, merchant_id int, product_id int, quantity int, primary key(id));
create table uproduct(id int, description varchar(128), primary key(id));
