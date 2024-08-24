Select IS_SRVROLEMEMBER('Sysadmin')
use dbETL

create table CustomerDetailsSrc 
(id int primary key identity, Name varchar(25) not null, country varchar(25) not null, phone varchar(10) not null, 
gender varchar(10) not null, created_at datetime, updated_at datetime);

select * from CustomerDetailsSrc

create table CustomerDetailsTgt
(id int, Name varchar(25) not null, country varchar(25) not null, phone varchar(10) not null, 
gender varchar(10) not null, created_at datetime, updated_at datetime, deleted_at datetime,
is_deleted bit not null);

select * from CustomerDetailsTgt

-- drop table CustomerDetailsSrc
-- drop table CustomerDetailsTgt

GO
CREATE OR ALTER PROCEDURE spETL 
(@debug_fl int = 0)
AS
BEGIN
    PRINT 'ETL execution';	

	-- Display Latest Updated Records
	with CTE as 
	(
		select *, ROW_NUMBER() over (partition by id order by created_at desc) as row_num from CustomerDetailsTgt t
	)

	select id, name, country, phone, gender, created_at, updated_at, deleted_at, is_deleted from CTE where row_num = 1;

	-- Inserted Records
	insert into CustomerDetailsTgt(id, name, country, phone, gender, created_at, updated_at, is_deleted)
	select s.id, s.name, s.country, s.phone, s.gender, s.created_at, s.updated_at, 0 from CustomerDetailsSrc s 
	left join CustomerDetailsTgt t on s.id = t.id where t.id is null;

	-- Deleted Records
	update CustomerDetailsTgt set is_deleted = 1, deleted_at = GETDATE() where id in
	(select t.id from CustomerDetailsTgt t left join CustomerDetailsSrc s on t.id = s.id where s.id is null and t.is_deleted = 0);

	-- Updated Records
	with CTE as 
	(
		select *, ROW_NUMBER() over (partition by id order by created_at desc) as row_num from CustomerDetailsTgt t
	)
	insert into CustomerDetailsTgt(id, name, country, phone, gender, created_at, updated_at, is_deleted)
	select s.id, s.name, s.country, s.phone, s.gender, GETDATE(), s.updated_at, 0 from CustomerDetailsSrc s inner join (select id, updated_at from CTE where row_num = 1) t on s.id = t.id where s.updated_at != t.updated_at;
END
GO

EXEC spETL

select * from CustomerDetailsSrc
select * from CustomerDetailsTgt

update CustomerDetailsSrc set phone = '9876543210', updated_at = getdate() where id = 1;
insert into CustomerDetailsSrc values ('Shara', 'India', '9999999999', 'Female', GETDATE(), GETDATE())
delete from CustomerDetailsSrc where id = 3 
