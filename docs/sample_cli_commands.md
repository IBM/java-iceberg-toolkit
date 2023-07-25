# CLI Commands

A list of examples of all supported CLI commands and their expected output. 

**Table of Contents**

* [Help](/docs/sample_cli_commands.md#help)
* [List](/docs/sample_cli_commands.md#list)
* [Create](/docs/sample_cli_commands.md#create)
* [Write/Commit/Rewrite](/docs/sample_cli_commands.md#writecommitrewrite)
* [Read](/docs/sample_cli_commands.md#read)
* [Table Information](/docs/sample_cli_commands.md#table-information)
* [Details](/docs/sample_cli_commands.md#details)
* [Rename](/docs/sample_cli_commands.md#rename)
* [Drop](/docs/sample_cli_commands.md#drop)

### Help 

* Display help for all commands.
```
% java -jar <jar> --help
usage: java -jar <jar_name> [options] command [args]
    --format <iceberg|hive>       The format of the table we want to
                                  display
 -h,--help                        Show this help message
 -o,--output <console|csv|json>   Show output in this format
 -u,--uri <value>                 Hive metastore to use
 -w,--warehouse <value>           Table location

Commands:
  drop                 Drop a table or a namespace
  schema               Fetch schema of a table
  read                 Read from a table
  commit               Commit file(s) to a table
  list                 List tables or namespaces
  uuid                 Fetch uuid of a table
  spec                 Fetch partition spec of a table
  rename               Rename a table a table
  create               Create a table or a namespace
  files                List data files of a table
  describe             Get details of a table or a namespace
  write                Write to a table
  snapshot             Fetch latest or all snapshot(s) of a table
```

* Display help for a specific command.
```
% java -jar <jar> -u <uri> list --help
usage: java -jar <jar_name> [options] list [options]

List tables or namespaces

Options:
  --help               Show this help message and exit

Positional Arguments:
  identifier           Table or namespace identifier
```

### List 

* List all namespaces and their locations in a catalog. 
```
% java -jar <jar> -u <uri> list
LIST OF NAMESPACES
NAMESPACE: LOCATION
test: s3a://<location>
test1: s3a://<location>
test2: hdfs://<location>
test3: s3a://<location>
```

* List all tables and their types in a namespace. Namespace *test* in this example.
```
% java -jar <jar> -u <uri> list test
LIST OF TABLES
TABLE NAME: TABLE TYPE
test_table: ICEBERG
test_table1: MANAGED_TABLE
test_table2: ICEBERG
```

### Create 

* Create a namespace. Namespace *test* in this example.
```
% java -jar <jar> -u <uri> -w <warehouse> create test
Namespace test created
```

* Create a table. Table *test_table* in namespace *test* in this example. Schema with ID, Name, Price, and Purchase_date columns in this example. 
```
% java -jar <jar> -u <uri> create test.test_table '{"type":"struct","schema-id":0,"fields":[{"id":1,"name":"ID","required":true,"type":"int"},{"id":2,"name":"Name","required":true,"type":"string"},{"id":3,"name":"Price","required":true,"type":"double"},{"id":4,"name":"Purchase_date","required":true,"type":"timestamp"}]}'
Creating the table test.test_table
Table created successfully
```

### Write/Commit/Rewrite 

* Write to a table. Table *test_table* in namespace *test* in this example. Adding one record to the schema in create table example in this example.
```
% java -jar <jar> -u <uri> write test.test_table '{"records":[{"ID":1,"Name":"Testing","Price": 1000,"Purchase_date":"2022-11-09T12:13:54.480"}]}'
Writing to the table test.test_table
New file created at: <file_location>
Commiting to the table test.test_table
Starting Txn
Txn Complete!
```

* Commit existing data files to a table. Table *test_table* in namespace *test* in this example.
```
% java -jar <jar> -u <uri> commit test.test_table '{"files":[{"file_path":"<path1>"}, {"file_path":"<path2>"}]}'
Commiting to the table test.test_table
Starting Txn
Txn Complete!
```

* Rewrite (replace) old data files in a table with new data files. Table *test_table* in namespace *test* in this example.
```
% java -jar <jar> -u <uri> rewrite test.test_table '{"files_to_del":[{"file_path":"path_a"}], "files_to_add":[{"file_path":"path_b"}]}'
Rewriting files in the table test.test_table
Starting Txn
Txn Complete!
```

### Read 

* Read from a table. Table *test_table* in namespace *test* in this example.
```
% java -jar <jar> -u <uri> read test.test_table 
Records in test.test_table :
1, Testing, 1000.0, 2022-11-09T12:13:54.480
```

### Table Information

* Fetch latest snapshot of a table. Table *test_table* in namespace *test* in this example.
```
% java -jar <jar> -u <uri> snapshot test.test_table 
CURRENT SNAPSHOT
BaseSnapshot{id=<id>, timestamp_ms=1671115114893, operation=append, summary={added-data-files=1, added-records=1, added-files-size=2000, changed-partition-count=1, total-records=1, total-files-size=2000, total-data-files=1, total-delete-files=0, total-position-deletes=0, total-equality-deletes=0}, manifest-list=<location>, schema-id=<id>}
```

* Fetch all snapshots of a table. Table *test_table* in namespace *test* in this example.
```
% java -jar <jar> -u <uri> snapshot --all test.test_table 
LIST OF SNAPSHOTS
BaseSnapshot{id=<id>, timestamp_ms=1671115114893, operation=append, summary={added-data-files=1, added-records=1, added-files-size=2000, changed-partition-count=1, total-records=1, total-files-size=2000, total-data-files=1, total-delete-files=0, total-position-deletes=0, total-equality-deletes=0}, manifest-list=<location>, schema-id=<id>}
```

* List data files of a table. Table *test_table* in namespace *test* in this example.
```
% java -jar <jar> -u <uri> files test.test_table 
TOTAL TASKS: 1
TOTAL FILES IN TASK 0 : 1
DATA <location> PARQUET 0 2000 [] true
```

* Get the partition spec of a table. Table *test_table1* in namespace *test* in this example.
```
% java -jar <jar> -u <uri> spec test.test_table1 
[
  1000: Birthday: identity(1)
]
```

* Get the schema of a table. Table *test_table* in namespace *test* in this example.
```
% java -jar <jar> -u <uri> schema test.test_table
SCHEMA
table {
  1: ID: required int
  2: Name: required string
  3: Price: required double
  4: Purchase_date: required timestamp
}
```

* Get the UUID of a table. Table *test_table* in namespace *test* in this example.
```
% java -jar <jar> -u <uri> uuid test.test_table
<uuid>
```

* Get record count of a table. Table *test_table* in namespace *test* in this example.
```
% java -jar <jar> -u <uri> recordcount test.test_table
<uuid>
```

### Details

* Get details of a table. Table *test_table* in namespace *test* in this example.
```
% java -jar <jar> -u <uri> describe test.test_table
TOTAL ESTIMATED RECORDS : 1
TOTAL TASKS: 1
TOTAL FILES IN TASK 0 : 1
DATA <location> PARQUET 0 2000 [] true

CURRENT SNAPSHOT
BaseSnapshot{id=<id>, timestamp_ms=1671115114893, operation=append, summary={added-data-files=1, added-records=1, added-files-size=2000, changed-partition-count=1, total-records=1, total-files-size=2000, total-data-files=1, total-delete-files=0, total-position-deletes=0, total-equality-deletes=0}, manifest-list=<location>, schema-id=<id>}

SCHEMA
table {
  1: ID: required int
  2: Name: required string
  3: Price: required double
  4: Purchase_date: required timestamp
}

TABLE LOCATION
<location>

DATA LOCATION
<location>
```

* Get details of a namespace. Namespace *test* in this example.
```
% java -jar <jar> -u <uri> describe test
DETAILS OF NAMESPACE
LOCATION : <location>
```

### Rename 

* Rename a table. Table *test_table* in namespace *test* to table *test_table_new* in namespace *test* in this example.
```
% java -jar <jar> -u <uri> rename test.test_table test.test_table_new
Table test.test_table renamed to test.test_table_new
```

### Drop 

* Drop a table. Table *test_table* in namespace *test* in this example.
```
% java -jar <jar> -u <uri> drop test.test_table
Dropping the table test.test_table
Table dropped successfully
```

* Drop a namespace. Namespace *test* in this example.
```
% java -jar <jar> -u <uri> drop test
Namespace test dropped
```




