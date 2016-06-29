CREATE TABLE helloworld (
   hello varchar(15),
   world varchar(15),
   dialect varchar(15) not null,
   PRIMARY KEY (dialect)
);
PARTITION TABLE helloworld ON COLUMN dialect;