create table merchant(id int, name varchar(128), category varchar(128), primary key(name, id));
create table morder(id int, user_id int, merchant_id int, product_id int, quantity int, primary key(id));
