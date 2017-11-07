add jar /home/msydoron/jethro-jdbc-3.3-standalone.jar;
CREATE EXTERNAL TABLE mytable2 (x INT)
STORED BY
'org.apache.hive.storage.jdbc.JdbcStorageHandler'
TBLPROPERTIES ( "hive.sql.database.type" = "JETHRO",
                "hive.sql.jdbc.driver" = "com.jethrodata.JethroDriver",
--                "hive.sql.jdbc.url" = "jdbc:JethroData://jonathan.dynqa.com:9111/sanity_tpcds",
                "hive.sql.jdbc.url" = "jdbc:JethroData://10.0.0.221:9111/demo3",
                "hive.sql.dbcp.username" = "jethro",
                "hive.sql.dbcp.password" = "jethro", 
                "hive.sql.query" = "select * from mytable",
                "hive.sql.table" = "mytable",
                "hive.sql.column.mapping" = "X=x",
                "hive.sql.dbcp.maxActive" = "1");
select * from mytable2 where x=10;

