drop view if exists vDummy;

drop function if exists pDummy_Insert(entity tDummy);
drop function if exists pDummy_Update(entity tDummy);
drop function if exists pDummy_InsertMany(entities tDummy[]);
drop function if exists pDummy_UpdateMany(entities tDummy[]);
drop function if exists pDummy_InsertOrIgnore(entity tDummy);
drop function if exists pDummy_InsertOrUpdate(entity tDummy);
drop function if exists pDummy_InsertOrIgnoreMany(entities tDummy[]);
drop function if exists pDummy_InsertOrUpdateMany(entities tDummy[]);

drop table if exists tDummy;