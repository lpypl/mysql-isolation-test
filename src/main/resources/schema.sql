create table table0 (pkId integer, pkAttr0 integer, pkAttr1 varchar(255), coAttr0 varchar(255), coAttr1 integer, coAttr2 varchar(255), primary key(pkAttr0, pkAttr1));
alter table table0 add index pk(pkAttr0, pkAttr1);