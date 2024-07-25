### Executing program

* How to run the program
* Run DDL Query
```
docker exec -ti local-spark bash -c '\
    $SPARK_HOME/bin/spark-submit \
    --master local[*] \
    --packages io.delta:${DELTA_PACKAGE_VERSION},org.apache.hadoop:hadoop-aws:3.3.2 \
    --conf spark.databricks.delta.retentionDurationCheck.enabled=false \
    --conf spark.sql.extensions=io.delta.sql.DeltaSparkSessionExtension \
    --conf spark.sql.catalog.spark_catalog=org.apache.spark.sql.delta.catalog.DeltaCatalog \
    --conf spark.hadoop.fs.s3a.access.key=minio \
    --conf spark.hadoop.fs.s3a.secret.key=minio123 \
    --conf spark.hadoop.fs.s3a.endpoint=http://minio:9000 \
    --conf spark.hadoop.fs.s3a.region=us-east-1 \
    --conf spark.hadoop.fs.s3a.impl=org.apache.hadoop.fs.s3a.S3AFileSystem \
    --conf spark.hadoop.fs.s3a.path.style.access=true \
    ./adventureworks/ddl/create_tables.py'
```

* Run ETL
```
docker exec -ti local-spark bash -c '\
    ${SPARK_HOME}/bin/spark-submit \
    --master local[*] \
    --packages io.delta:${DELTA_PACKAGE_VERSION},org.apache.hadoop:hadoop-aws:3.3.2 \
    --conf spark.databricks.delta.retentionDurationCheck.enabled=false \
    --conf spark.sql.extensions=io.delta.sql.DeltaSparkSessionExtension \
    --conf spark.sql.catalog.spark_catalog=org.apache.spark.sql.delta.catalog.DeltaCatalog \
    --conf spark.hadoop.fs.s3a.access.key=minio \
    --conf spark.hadoop.fs.s3a.secret.key=minio123 \
    --conf spark.hadoop.fs.s3a.endpoint=http://minio:9000 \
    --conf spark.hadoop.fs.s3a.region=us-east-1 \
    --conf spark.hadoop.fs.s3a.impl=org.apache.hadoop.fs.s3a.S3AFileSystem \
    --conf spark.hadoop.fs.s3a.path.style.access=true \
    ./adventureworks/pipelines/sales_mart.py \
'
```


* Run spark-shell to access sql command
```
docker exec -ti local-spark bash -c '\
    $SPARK_HOME/bin/spark-shell \
    --packages io.delta:${DELTA_PACKAGE_VERSION},org.apache.hadoop:hadoop-aws:3.3.2 \
    --conf spark.hadoop.fs.s3a.access.key=minio \
    --conf spark.hadoop.fs.s3a.secret.key=minio123 \
    --conf spark.hadoop.fs.s3a.endpoint=http://minio:9000 \
    --conf spark.hadoop.fs.s3a.region=us-east-1 \
    --conf spark.hadoop.fs.s3a.impl=org.apache.hadoop.fs.s3a.S3AFileSystem \
    --conf spark.hadoop.fs.s3a.path.style.access=true \
    --conf spark.databricks.delta.retentionDurationCheck.enabled=false \
    --conf spark.sql.extensions=io.delta.sql.DeltaSparkSessionExtension \
    --conf spark.sql.catalog.spark_catalog=org.apache.spark.sql.delta.catalog.DeltaCatalog'
```

```
spark.sql("select partition from adventureworks.sales_mart group by 1").show() // should be the number of times you ran `make etl`
spark.sql("select count(*) from businessintelligence.sales_mart").show() // 59
spark.sql("select count(*) from adventureworks.dim_customer").show() // 1000 * num of etl runs
spark.sql("select count(*) from adventureworks.fct_orders").show() // 10000 * num of etl runs
```

* Run test
```
docker exec -ti local-spark bash
```

```
pytest --log-cli-level info -p no:warnings -v ./adventureworks/tests
```
https://delta.io/blog/delta-lake-s3/
https://docs.delta.io/latest/delta-batch.html#create-a-table&language-python
