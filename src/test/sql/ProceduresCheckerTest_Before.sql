create table if not exists GeoPoint(latitude float8 not null, longitude float8 not null);
create table if not exists GeoPoint3D(latitude float8 not null, longitude float8 not null, altitude float8 null);

drop function if exists pDummyTypeChecked1(arg0 integer);
drop function if exists pDummyTypeChecked2(entity GeoPoint);
drop function if exists pDummyTypeChecked3(entities GeoPoint[]);
drop function if exists pDummyTypeChecked4(arg0 integer);
drop function if exists pDummyTypeChecked5(arg0 integer, arg1 integer);

create or replace function pDummyTypeChecked1(arg0 integer) returns bigint as $$
  begin
    return 1;
  end;
$$ language plpgsql;

create or replace function pDummyTypeChecked2(entity GeoPoint) returns bigint as $$
  begin
    return 1;
  end;
$$ language plpgsql;

create or replace function pDummyTypeChecked3(entities GeoPoint[]) returns bigint as $$
  begin
    return 1;
  end;
$$ language plpgsql;

create or replace function pDummyTypeChecked4(arg0 integer) returns table(latitude float8, longitude float8, altitude float8) as $$
  begin
    return query select 0::float8, 0::float8, null::float8;
  end;
$$ language plpgsql;

create or replace function pDummyTypeChecked5(arg0 integer, arg1 integer) returns setof GeoPoint3D as $$
  begin
   return query select 0::float8, 0::float8, null::float8;
  end;
$$ language plpgsql;