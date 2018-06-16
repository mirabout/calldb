-- This function should return set (float8, float8) tuples with names "latitude", "longitude"
create or replace function DummyResultSetRoutine() returns table(latitude float8, longitude float8) as $$
  begin
    return query select 48.0::float8, 44.0::float8;
  end;
$$ language plpgsql;

-- This procedure should just return 0 as a bigint
create or replace function DummyProcedure0() returns bigint as $$
  begin
    return 0;
  end;
$$ language plpgsql;

-- This procedure should just return 1 as a bigint
create or replace function DummyProcedure1(arg0_ text) returns bigint as $$
  begin
    return 1;
  end;
$$ language plpgsql;

-- This procedure should just return 2 as a bigint
create or replace function DummyProcedure2(arg0_ integer, arg1_ integer) returns bigint as $$
  begin
    return 2;
  end;
$$ language plpgsql;

-- This procedure should just return 3 as a bigint
create or replace function DummyProcedure3(arg0_ integer, arg1_ integer, arg2_ integer) returns bigint as $$
  begin
    return 3;
  end;
$$ language plpgsql;

-- This procedure should just return 4 as a bigint
create or replace function DummyProcedure4(arg0_ integer, arg1_ integer, arg2_ integer, arg3_ integer) returns bigint as $$
  begin
    return 4;
  end;
$$ language plpgsql;

-- This function should return set of all postgresql type oids as integers
create or replace function DummyFunction0() returns setof integer as $$
  begin
     return query select pg_type.oid::integer from pg_type;
  end;
$$ language plpgsql;

-- This function should return length of a given string as a bigint
create or replace function DummyFunction1(arg0_ text) returns bigint as $$
  begin
    return character_length(arg0_);
  end;
$$ language plpgsql;

-- This function should return product of its args as a double precision value
create or replace function DummyFunction2(arg0_ float8, arg1_ float8) returns float8 as $$
  begin
    return arg0_ * arg1_;
  end;
$$ language plpgsql;

-- This function should return value of arg0 clamped by [arg1, arg2] bounds as a double precision value
create or replace function DummyFunction3(arg0_ float8, arg1_ float8, arg2_ float8) returns float8 as $$
  begin
    if arg0_ < arg1_ then
      return arg1_;
    elsif arg0_ > arg2_ then
      return arg2_;
    else
      return arg0_;
    end if;
  end;
$$ language plpgsql;
