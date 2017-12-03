add jar /home/msydoron/jethro-jdbc-3.3-standalone.jar;
CREATE EXTERNAL TABLE ext_mytable1 (x1 INT, y1 DOUBLE)
STORED BY
'org.apache.hive.storage.jdbc.JdbcStorageHandler'
TBLPROPERTIES ( "hive.sql.database.type" = "JETHRO",
                "hive.sql.jdbc.driver" = "com.jethrodata.JethroDriver",
                "hive.sql.jdbc.url" = "jdbc:JethroData://10.0.0.221:9111/demo3",
                "hive.sql.dbcp.username" = "jethro",
                "hive.sql.dbcp.password" = "jethro", 
                "hive.sql.table" = "mytable1",
                "hive.sql.dbcp.maxActive" = "1");
                
CREATE EXTERNAL TABLE ext_mytable2 (x2 INT, y2 DOUBLE)
STORED BY
'org.apache.hive.storage.jdbc.JdbcStorageHandler'
TBLPROPERTIES ( "hive.sql.database.type" = "JETHRO",
                "hive.sql.jdbc.driver" = "com.jethrodata.JethroDriver",
                "hive.sql.jdbc.url" = "jdbc:JethroData://10.0.0.221:9111/demo3",
                "hive.sql.dbcp.username" = "jethro",
                "hive.sql.dbcp.password" = "jethro", 
                "hive.sql.table" = "mytable2",
                "hive.sql.dbcp.maxActive" = "1");                


SELECT ext_mytable1.x1, ext_mytable1.y1, ext_mytable2.x2
FROM ext_mytable1
INNER JOIN ext_mytable2 ON ext_mytable1.x1=ext_mytable2.x2;

SELECT ext_mytable1.x1, ext_mytable1.y1, ext_mytable2.x2
FROM ext_mytable1
JOIN ext_mytable2 ON ext_mytable1.x1=ext_mytable2.x2 where sqrt(x1*y1)   = sqrt(x2*y2) and 
														   sqrt(x1*x2)   = sqrt(y1*y2) and
 													       bround(x1*y1) = bround(x2*y2);


                
--select count(*) from ext_mytable where x=10 group by x;
--
--select count(x) from ext_mytable where x!=10;
--select y,x from ext_mytable where x=10;
----select x,count(*) from ext_mytable where x=10;
--select x,y from ext_mytable where bround(x*y)=10;
--
--
--select x,y*y from ext_mytable where x*x!=100;
--
--select x, count(x) from ext_mytable where x!=10 group by x;
--
----select x, count(*) from ext_mytable where x==10 group by x;
--
--select sum(x) from ext_mytable;

