
create table addr (
id_addr bigserial not null primary key,
addr varchar(128) NOT NULL
);

create index addr_idx01 on addr(addr);

create table domain(
id_domain bigserial not null primary key,
domain varchar(128) not null
);

create index domain_idx01 on domain(domain);

create table event(
id_event bigserial not null primary key,
ts bigint not null,
fk_src_addr bigint default null references addr,
fk_src_domain bigint default null references domain
);

create index event_idx01 on event(ts);
