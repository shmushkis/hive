add jar /home/msydoron/jethro-jdbc-3.3-standalone.jar;
CREATE EXTERNAL TABLE ext_mytable (x INT, y DOUBLE)
STORED BY
'org.apache.hive.storage.jdbc.JdbcStorageHandler'
TBLPROPERTIES ( "hive.sql.database.type" = "JETHRO",
                "hive.sql.jdbc.driver" = "com.jethrodata.JethroDriver",
--                "hive.sql.jdbc.url" = "jdbc:JethroData://jonathan.dynqa.com:9111/sanity_tpcds",
                "hive.sql.jdbc.url" = "jdbc:JethroData://10.0.0.221:9111/demo3",
                "hive.sql.dbcp.username" = "jethro",
                "hive.sql.dbcp.password" = "jethro", 
                "hive.sql.query" = "select x,y from mytable",
                "hive.sql.table" = "mytable",
--                "hive.sql.column.mapping" = "x2=x,y2=y",
                "hive.sql.dbcp.maxActive" = "1");
--select x, count(*) from ext_mytable where x=10 group by x;

select x,y from ext_mytable where x=10;
select x,y from ext_mytable where bround(x*y)=10;
select y,x from ext_mytable where x=10;

select x,y*y from ext_mytable where x*x!=100;

select x, count(x) from ext_mytable where x!=10 group by x;

--select x, count(*) from ext_mytable where x==10 group by x;

select sum(x) from ext_mytable;

