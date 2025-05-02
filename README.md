# Iceberg output plugin for Embulk

embulk-output-iceberg is the Embulk output plugin for Apache Iceberg.

## Overview
Required Embulk version >= 0.11.5.  
Java 11. iceberg API support Java 11 above. (Despite Embulk official support is Java 8)

* **Plugin type**: output
* **Resume supported**: no
* **Cleanup supported**: no
* **Guess supported**: no

## Configuration
Now Only support REST Catalog with MinIO Storage, Glue Catalog and JDBC Catalog.

### Embulk Configuration
- **catalog_name** catalog name. if jdbc need to set correct catalog name. (string, optional)
- **namespace** catalog namespace name. if glue set glue database. (string, required)
- **table** catalog table name (string, required)
- **catalog_type** catalog type. "REST", "JDBC", "GLUE" is available. (string, required)
- **uri** catalog uri. if "REST" use http URI scheme. if "JDBC" use JDBC driver URI.  (string, required)
- **warehouse_location** warehouse to save data. if use S3, URI scheme.  (string, required)
- **file_io_impl** implementation of file io.  (string, required)
- **endpoint**: Object Storage endpoint. if set path_style_access true, actual path is  like "http://localhost:9000/warehouse/" (string, required)
- **path_style_access**: use path url (string, required)
- **jdbc_driver_path**: jdbc driver jar file path. (string, optional)
- **jdbc_driver_class_name**: jdbc class name (string, optional)
- **jdbc_user**: jdbc database user name (string, optional)
- **jdbc_pass**: jdbc database password (string, optional)
- **file_format**: iceberg datafile format. "PARQUET" "ORC" "AVRO" is available (string, optional default "PARQUET")
- **file_size**: iceberg datafile size. (maybe before compression size) (long, optional default 134217728(250MB))
- **mode**: "APPEND" "DELETE_APPEND" is available. "APPEND" means only add data. "DELETE_APPEND" means delete all data first and add data. (string, optional default "APPEND")

### environment
When access Object Storage, normally use `org.apache.iceberg.aws.s3.S3FileIO`.  
We need to set Environment Variable below to access Object Storage.
- AWS_REGION
- AWS_ACCESS_KEY_ID
- AWS_SECRET_ACCESS_KEY

## Example
1. Only append data.
```yaml
out: 
  type:
    source: maven
    group: io.github.shin1103 
    name: iceberg 
    version: 0.2.0
  catalog_name: "pg-iceberg"
  namespace: "taxi"
  table: "taxi_dataset_copy"
  catalog_type: "jdbc"
  jdbc_driver_path: "C:\\lib\\.my_local\\postgresql-42.7.5.jar"
  jdbc_driver_class_name: "org.postgresql.Driver"
  jdbc_user: "user"
  jdbc_pass: "password"
  uri: "jdbc:postgresql://localhost:5432/postgres"
  warehouse_location: "s3://iceberg/"
  file_io_impl: "org.apache.iceberg.aws.s3.S3FileIO"
  endpoint: "http://localhost:9000/"
  path_style_access: "true"
  mode: "APPEND" 
```

2.  Delete all data and append data.
```yaml
out:
  type:
    source: maven
    group: io.github.shin1103
    name: iceberg
    version: 0.2.0
  catalog_name: "pg-iceberg"
  namespace: "taxi"
  table: "taxi_dataset_copy"
  catalog_type: "jdbc"
  jdbc_driver_path: "C:\\lib\\.my_local\\postgresql-42.7.5.jar"
  jdbc_driver_class_name: "org.postgresql.Driver"
  jdbc_user: "user"
  jdbc_pass: "password"
  uri: "jdbc:postgresql://localhost:5432/postgres"
  warehouse_location: "s3://iceberg/"
  file_io_impl: "org.apache.iceberg.aws.s3.S3FileIO"
  endpoint: "http://localhost:9000/"
  path_style_access: "true"
  mode: "DELETE_APPEND"
```

3. Change file format and file size.
Multi filter is available.
```yaml
out:
  type:
    source: maven
    group: io.github.shin1103
    name: iceberg
    version: 0.2.0
  catalog_name: "pg-iceberg"
  namespace: "taxi"
  table: "taxi_dataset_copy"
  catalog_type: "jdbc"
  jdbc_driver_path: "C:\\lib\\.my_local\\postgresql-42.7.5.jar"
  jdbc_driver_class_name: "org.postgresql.Driver"
  jdbc_user: "user"
  jdbc_pass: "password"
  uri: "jdbc:postgresql://localhost:5432/postgres"
  warehouse_location: "s3://iceberg/"
  file_io_impl: "org.apache.iceberg.aws.s3.S3FileIO"
  endpoint: "http://localhost:9000/"
  path_style_access: "true"
  file_format: "ORC"
  file_size: 2097152
```


## Types
Types are different from [iceberg](https://iceberg.apache.org/spec/#primitive-types) and [Embulk](https://www.embulk.org/docs/built-in.html).
So some iceberg type isn't supported.

Unsupported Iceberg Types
- FIXED
- BINARY
- STRUCT
- LIST
- MAP
- VARIANT
- UNKNOWN
- TIMESTAMP_NS
- TIMESTAMPTZ_NS
- GEOMETRY
