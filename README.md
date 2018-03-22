# Spark Data Sources and Spark SQL
This is a simple application that converts different data sources into Parquet.

## Requirements
- Spark 2.3
- Scala 2.11

## Compilation
```bash
$ sbt package
```

## Run Application
```bash
$ YOUR_SPARK_HOME/bin/spark-submit 
    --class <main-class>
    --master <master-url>
    target/scala-2.11/simple-project_2.11-1.0.jar
```

- class: The entry point for your application (e.g. Json2Parquet)
- master: The master URL for the cluster (e.g. spark://23.195.26.187:7077 or local[*] run in local mode)
