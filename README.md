# how-to-join-two-hudi-tables-and-insert-aggreates-or-metrics-into-DynamoDB-Table-

![diagram drawio](https://github.com/soumilshah1995/how-to-join-two-hudi-tables-and-insert-aggreates-or-metrics-into-DynamoDB-Table-/assets/39345855/b01f821e-5371-4845-8893-c8da9a06e4dc)

```
--additional-python-modules | faker==11.3.0,pynamodb

--conf  |  spark.serializer=org.apache.spark.serializer.KryoSerializer  --conf spark.sql.hive.convertMetastoreParquet=false --conf spark.sql.hive.convertMetastoreParquet=false --conf spark.sql.catalog.spark_catalog=org.apache.spark.sql.hudi.catalog.HoodieCatalog --conf spark.sql.legacy.pathOptionBehavior.enabled=true --conf spark.sql.extensions=org.apache.spark.sql.hudi.HoodieSparkSessionExtension

--datalake-formats | hudi

```
