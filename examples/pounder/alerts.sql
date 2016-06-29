CREATE TABLE alerts
(
  id bigint not null,
  msg varchar(128),
  Event_Timestamp TIMESTAMP DEFAULT NOW,
);

partition table alerts on column id;

create procedure inalerts partition
     on table alerts column id
  as insert into alerts (id,msg) values (?,?);
