--! qt:dataset:src
CREATE TABLE dest1(key INT, value STRING) STORED AS TEXTFILE;

EXPLAIN
FROM (
  FROM src
  SELECT TRANSFORM(src.key, src.value) ROW FORMAT DELIMITED FIELDS TERMINATED BY '\002'
  USING 'cat'
  AS (tkey, tvalue) ROW FORMAT DELIMITED FIELDS TERMINATED BY '\003'
) tmap
INSERT OVERWRITE TABLE dest1 SELECT tkey, tvalue;

FROM (
  FROM src
  SELECT TRANSFORM(src.key, src.value) ROW FORMAT DELIMITED FIELDS TERMINATED BY '\002'
  USING 'cat'
  AS (tkey, tvalue) ROW FORMAT DELIMITED FIELDS TERMINATED BY '\003'
) tmap
INSERT OVERWRITE TABLE dest1 SELECT tkey, tvalue;

SELECT dest1.* FROM dest1;
