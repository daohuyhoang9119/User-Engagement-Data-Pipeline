SELECT COUNT("Contract") as "Total", 
	SUM("Total_Giải_Trí") as "Giải Trí", SUM("Total_Thể_Thao") as "Thể Thao",  
	SUM("Total_Phim_Truyện") as "Phim Truyện", SUM("Total_Thiếu_Nhi") as "Thiếu Nhi",
	SUM("Total_Truyền_Hình") as "Truyền Hình", '2022-04-01'::date AS Date
from user_log;


create procedure auto_import_data_user_daily()
	begin
	set @date = date(now())
	insert into public."data_warehouse"."daily_user_statistics"
	SELECT COUNT("Contract") as "Total", 
	SUM("Total_Giải_Trí") as "Giải Trí", SUM("Total_Thể_Thao") as "Thể Thao",  
	SUM("Total_Phim_Truyện") as "Phim Truyện", SUM("Total_Thiếu_Nhi") as "Thiếu Nhi",
	SUM("Total_Truyền_Hình") as "Truyền Hình", '2022-04-01'::date AS Dateg
	from user_log
	end;
	
	
SELECT * FROM pg_stat_activity;