# Iceberg input plugin for Embulk

embulk-input-iceberg is the Embulk input plugin for Apache Iceberg.

## Overview
Required Embulk version >= 0.11.5.  
Java 11. iceberg API support Java 11 above. (Despite Embulk official support is Java 8)

* **Plugin type**: input
* **Resume supported**: no
* **Cleanup supported**: no
* **Guess supported**: no

## Configuration
Now Only support REST Catalog with MinIO Storage, and Glue Catalog.

### Embulk Configuration
- **catalog_name** catalog name. if jdbc need to set collect catalog name. (string, optional)
- **namespace** catalog namespace name. if glue set glue database. (string, required)
- **table** catalog table name (string, required)
- **catalog_type** catalog type. use "REST" (string, required)
- **uri** catalog uri. if "REST" use http URI scheme. if "JDBC" use JDBC driver URI.  (string, required)
- **warehouse_location** warehouse to save data. if use S3, URI scheme.  (string, required)
- **file_io_impl** implementation of file io.  (string, required)
- **endpoint**: Object Storage endpoint. if set path_style_access true, actual path is  like "http://localhost:9000/warehouse/" (string, required)
- **path_style_access**: use path url (string, required)
- **decimal_as_string**: if true, treat decimal as string. else treat as double (boolean, optional, default false)
- **jdbc_driver_path**: jdbc driver jar file path. (string, optional)
- **jdbc_driver_class_name**: jdbc class name (string, optional)
- **jdbc_user**: jdbc database user name (string, optional)
- **jdbc_pass**: jdbc database password (string, optional)
- **table_filters**: filter rows. support filter is predicate expressions only. [expressions](https://iceberg.apache.org/docs/1.8.1/api/#expressions) (list, optional)
- **columns**: select column name list. if not define, all columns are selected.  (list, optional)

### environment
When access Object Storage, normally use `org.apache.iceberg.aws.s3.S3FileIO`.  
We need to set Environment Variable below to access Object Storage.
- AWS_REGION
- AWS_ACCESS_KEY_ID
- AWS_SECRET_ACCESS_KEY

## Example
1. Select All rows and columns.
```yaml
in:
  type:
    source: maven
    group: io.github.shin1103
    name: iceberg
    version: 0.1.0
  namespace: "n_space"
  table: "my_table_2"
  catalog_type: "rest"
  uri: "http://localhost:8181"
  warehouse_location: "s3://warehouse/"
  file_io_impl: "org.apache.iceberg.aws.s3.S3FileIO"
  endpoint: "http://localhost:9000/"
  path_style_access: "true"
```

2.  Select particular columns only.
```yaml
in:
  type:
    source: maven
    group: io.github.shin1103
    name: iceberg
    version: 0.1.0
  namespace: "n_space"
  table: "my_table_2"
  catalog_type: "rest"
  uri: "http://localhost:8181"
  warehouse_location: "s3://warehouse/"
  file_io_impl: "org.apache.iceberg.aws.s3.S3FileIO"
  endpoint: "http://localhost:9000/"
  path_style_access: "true"
  columns:
    - id
    - name
```

3. Filter rows
Multi filter is available.
```yaml
in:
  type:
    source: maven
    group: io.github.shin1103
    name: iceberg
    version: 0.1.0
  namespace: "n_space"
  table: "my_table_2"
  catalog_type: "rest"
  uri: "http://localhost:8181"
  warehouse_location: "s3://warehouse/"
  file_io_impl: "org.apache.iceberg.aws.s3.S3FileIO"
  endpoint: "http://localhost:9000/"
  path_style_access: "true"
  decimal_as_string: true
  table_filters:
    - {type: ISNULL, column: id}
    - {type: EQUAL, column: id, value: 4}
    - {type: GREATERTHAN, column: id, value: 2}
    - {type: IN, column: id, in_values: [2, 3]}
```

4. Use Glue catalog
```yaml
in:
  type:
    source: maven
    group: io.github.shin1103
    name: iceberg
    version: 0.1.0
  namespace: "my_database" # Set Glue Database
  table: "my_table_2"
  catalog_type: "glue"
  warehouse_location: "s3://warehouse/"
  file_io_impl: "org.apache.iceberg.aws.s3.S3FileIO"
```

5. Use JDBC catalog with MinIO
```yaml
in:
  type:
    source: maven
    group: io.github.shin1103
    name: iceberg
    version: 0.1.0
  catalog_name: "taxi"
  namespace: "n_space"
  table: "taxi_list"
  catalog_type: "jdbc"
  jdbc_driver_path: "C:\\lib\\sqlite-jdbc-3.49.1.0.jar"
  jdbc_driver_class_name: "org.sqlite.JDBC"
  jdbc_user: "user"
  jdbc_pass: "password"
  uri: "jdbc:sqlite:C://warehouse/iceberg_catalog.db"
  warehouse_location: "s3://iceberg/"
  file_io_impl: "org.apache.iceberg.aws.s3.S3FileIO"
  endpoint: "http://localhost:9000/"
  path_style_access: "true"
  decimal_as_string: true
```

## Types
Types are different from [iceberg](https://iceberg.apache.org/spec/#primitive-types) and [Embulk](https://www.embulk.org/docs/built-in.html).
So some iceberg type isn't supported.

Unsupported Iceberg Types
- UUID
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
