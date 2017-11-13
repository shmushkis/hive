add jar /home/msydoron/jethro-jdbc-3.3-standalone.jar;
CREATE EXTERNAL TABLE mytable2 (x2 INT, y2 DOUBLE)
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
                "hive.sql.column.mapping" = "x2=x,y2=y",
                "hive.sql.dbcp.maxActive" = "1");
select x2, count(*) from mytable2 where x2=10 group by x2;
select sum(x2) from mytable2;

