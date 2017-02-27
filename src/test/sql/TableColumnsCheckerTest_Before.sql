-- Weird case that may be met is intended to check proper case-insensitivity handling

drop table if exists tDummyWithPK;
drop table if exists tDummyNoPK;

create table tDummyWithPK(
  storeId uuid not null,
  foreignId uuid not null,
  description varchar(255) not null,
  weighT float8 null,
  cosT float8 null,
  Images bytea[] null,
  primary key(storeId, foreignId)
);

create table tDummyNoPK(
  Latitude float8 not null,
  Longitude float8 not null,
  altitude float8 null,
  gpsTime bigint not null,
  attributes hstore null
);
