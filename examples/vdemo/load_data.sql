insert into user(id, name, balance) values(1, 'sougou', 10);
insert into user(id, name, balance) values(6, 'demmer', 20);
insert into merchant(name, category) values('monoprice', 'electronics');
insert into merchant(name, category) values('newegg', 'electronics');
insert into product(id, description) values(1, 'keyboard');
insert into product(id, description) values(2, 'monitor');
insert into uorder(id, uid, mname, pid, price) values(1, 1, 'monoprice', 1, 10);
insert into uorder(id, uid, mname, pid, price) values(2, 1, 'newegg', 2, 15);
insert into uorder(id, uid, mname, pid, price) values(3, 6, 'monoprice', 2, 20);
