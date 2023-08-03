# java-iceberg-toolkit
java-iceberg-toolkit is a Java implementation for performing operations on Apache Iceberg and Hive tables to enable open data lakehouse access to developers, data scientists and DB users. For a detailed list of supported operations, refer to [Supported Operations](/README.md#supported-operations).

One of the common use-cases of the toolkit is bulk ingestion of parquet files to a data lake in Iceberg table format. For more information, visit ["A java toolkit for Apache Iceberg open table format"](https://medium.com/@pandey.brajesh/a-java-toolkit-for-apache-iceberg-open-table-format-64c329d6c719).

**Table of Contents**
* [Pre-Requisites](/README.md#pre-requisites)
* [Install and Build](/README.md#install-and-build)
* [Configuration](/README.md#configuration)
* [CLI](/README.md#cli-2)
* [Server Mode](/README.md#server-mode)
* [API](/README.md#api)
* [Limitations](/README.md#limitations)


## Pre-Requisites

1. Hive Metastore 4
2. An object store (Amazon S3, IBM COS, etc.)
3. Credentials to access the bucket (if not public), refer to [Configuration](/README.md#configuration)
4. Java version 17 and above
5. Apache Maven

### CLI

1. Apache Maven Dependencies 
2. Credentials set as environment variables 

### Testing

1. JUnit5
2. Credentials set as environment variable
3. URI set as "URI" environment variable
4. Warehouse set as "WAREHOUSE" environment variable

## Install and Build 

1- Build an image either using the provided script or a container engine's (e.g. docker, podman) CLI of your choice. For this example, we are using podman.
```
# using provided script
./createImages.sh

# using preferred container engine's cli remove and un-tag an existing image before building a new one
podman rmi -f java-iceberg-cli:latest
podman build --tag java-iceberg-cli:latest --file Dockerfile . 
```

2- Create a container and run it as a detached process
```
podman run -d localhost/java-iceberg-cli:latest
```

3- Open a remote shell to the container
```
podman exec -it <container_name> bash
```

4- Run the Java tool
```
java -jar /home/java-iceberg-cli/target/<jar_name> --help
```

## Configuration

### CLI
Pass in the URI to the Hive Metastore using -u or --uri options and optionally specify a storage path when creating a new namespace using -w or --warehouse options. By default, the default FS value specified in Hive's configuration file will be used as the warehouse path.

Set credentials for the object store using environment variables. Please note that you would need to specify AWS_ENDPOINT if using a non-AWS object store.
```
export AWS_ACCESS_KEY_ID=
export AWS_SECRET_ACCESS_KEY=
export AWS_REGION=
# specify endpoint to a non-AWS object store, if applicable
export AWS_ENDPOINT=
```

Credentials can also be passed to the CLI as:
```
{'type':'AWS','credentials':{'AWS_ACCESS_KEY_ID':'<id>','AWS_SECRET_ACCESS_KEY':'<key>', 'ENDPOINT':'uri'}}
```

### Unit Tests
Set credentials using environment variables as:
```
export AWS_ACCESS_KEY_ID=
export AWS_SECRET_ACCESS_KEY=
export AWS_REGION=
# specify endpoint to a non-AWS object store, if applicable
export AWS_ENDPOINT=
export URI=
export WAREHOUSE=
```
If URI is not set as environment variables then, the tests will exit. 

## CLI

java-iceberg-toolkit comes with a CLI which is ready to use when the code is packaged and all configurations are in place.

### Quickstart

The CLI provides various operations on Iceberg and Hive tables.
```
$ java -jar <jar_name> --help
usage: java -jar <jar_name> [options] command [args]
 -c,--credential <credentials>    Supported credentials : AWS
    --catalog <value>             Read properties for this catalog from
                                  the config file
    --format <iceberg|hive>       The format of the table we want to
                                  display
 -h,--help                        Show this help message
 -o,--output <console|csv|json>   Show output in this format
    --snapshot <snapshot ID>      Snapshot ID to use
 -u,--uri <value>                 Hive metastore to use
 -w,--warehouse <value>           Table location

Commands:
  drop                 Drop a table or a namespace
  schema               Fetch schema of a table
  metadata             Get table metadata
  read                 Read from a table
  commit               Commit file(s) to a table
  rewrite              Rewrite file(s) in a table
  list                 List tables or namespaces
  type                 Fetch table type
  uuid                 Fetch uuid of a table
  spec                 Fetch partition spec of a table
  rename               Rename a table a table
  create               Create a table or a namespace
  files                List data files of a table
  location             Fetch table location
  describe             Get details of a table or a namespace
  write                Write to a table
  snapshot             Fetch latest or all snapshot(s) of a table
  tasks                List scan tasks of a table
```
Each subcommand provides a help message of its own.
```
usage: java -jar <jar_name> [options] create [options] identifier

Create a table or a namespace

Options:
  --help               Show this help message and exit
  --force              If table exists, recreate an empty table

Positional Arguments:
  identifier           Table or namespace identifier
  schema               Create a table using this schema
```
### Config File

The toolkit allows users to specify catalog configuration using a config file. The file is expected to be named as .java_iceberg_cli.yaml and is searched in the following locations:
1- ICEBERG_CONFIG environment variable
2- User home directory

### Security

Credentials for AWS can be passed to the CLI as:
```
{'type':'AWS','credentials':{'AWS_ACCESS_KEY_ID':'<id>','AWS_SECRET_ACCESS_KEY':'<key>', 'ENDPOINT':'uri'}}
```

#### Kerberos
To use Kerberos enabled Metastore you can use any of the following two options:

1- Set up values in the config file:
```
catalogs:
- name: "default"
  type: "HIVE"
  metastoreUri: <uri>
  properties: {...}
  conf:
   ...
   hadoop.security.authentication: "kerberos"
   hive.metastore.sasl.enabled: "true"
   hive.metastore.kerberos.principal: "<principal>"
   hive.metastore.kerberos.keytab.file: "<path_to_keytab"
   ...
```

2- Set up the following environment variables:
```
HADOOP_AUTHENTICATION="kerberos"
METASTORE_SASL_ENABLED="true"
```
Specify kerberos principal and keytab as:
```
METASTORE_KERBEROS_PRINCIPLE=<principal>
METASTORE_KERBEROS_KEYTAB=<path_to_keytab>
```
Or as:
```
KRB5PRINCIPAL=<principal>
KRB5KEYTAB=<path_to_keytab>
```

### Plain Authentication

To access Metastore that has PLAIN authentication mode enabled, there are two options:

1- Set up values in the config file:
```
catalogs:
- name: "default"
  type: "HIVE"
  metastoreUri: <uri>
  properties: {...}
  conf:
   ...
   hive.metastore.client.plain.username: "<username>"
   hive.metastore.client.plain.password: "<pw>"
   ...
```

### Connecting to SSL enabled Metastore

To access SSL enabled Metastore, there are two options:

1- Set up values in the config file:
```
catalogs:
- name: "default"
  type: "HIVE"
  metastoreUri: <uri>
  properties: {...}
  conf:
   ...
   hive.metastore.use.SSL: "true"
   hive.metastore.keystore.path: "<path>"
   hive.metastore.keystore.password: "<pw>"
   hive.metastore.truststore.path: "<path>"
   hive.metastore.truststore.password: "<pw>"
   ...
```

2- Set up the following environment variables:
```
METASTORE_SSL_ENABLED="true"
METASTORE_KEYSTORE_PATH=<path_to_keystore>
METASTORE_KEYSTORE_PASSWORD=<password>
METASTORE_TRUSTSTORE_PATH=<path_to_truststore>
METASTORE_TRUSTSTORE_PASSWORD=<password>
```

### Sample CLI Commands
<details><summary>Example CLI commands for basic queries.</summary>

1. Display help for all commands.
```
java -jar <jar> --help
```
2. Display help for a specific command.
```
java -jar <jar> -u <uri> <command> --help
```
3. List all namespaces and their locations in a catalog. 
```
java -jar <jar> -u <uri> list
```
4. List all tables and their types in a namespace.
```
java -jar <jar> -u <uri> list <namespace>
```
5. Create a table
```
java -jar <jar> -u <uri> create <namespace>.<table> "<schema>"
```
* Example schema
```
'{"type":"struct","schema-id":0,"fields":[{"id":1,"name":"ID","required":true,"type":"int"},{"id":2,"name":"Name","required":true,"type":"string"},{"id":3,"name":"Price","required":true,"type":"double"},{"id":4,"name":"Purchase_date","required":true,"type":"timestamp"}]}'
```
</details>

For a detailed list of commands, please refer to [sample cli commands](docs/sample_cli_commands.md).

### Supported Operations

Most of the operations on Hive tables are being worked on. For contributions, please refer to the [contribution guidelines](docs/contribution_guidelines.md) for this project. 

Operation Name | Iceberg Table | Hive Table
---|---|---|
Create a namespace or a table | Y |
Commit to a table | Y |
Rewrite files in a table | Y |
Describe a namespace or a table | Y
Drop a namespace or a table | Y 
Get plan tasks of a table | Y | Y
Get plan files of a table | Y |
Get schema of a table | Y | Y
Get uuid of a table | Y |
Get partition spec of a table | Y |
Get table metadata | Y |
Get current or all snapshots of a table | Y |
List namespaces | Y | Y
List tables in a namespace | Y | Y
List tables in all namespaces | Y |
Read from a table | Y
Rename a table | Y |
Write to a table | Y |

## Server Mode

java-iceberg-toolkit provides a Server mode which uses UNIX domain sockets. To start the server run:
```
java -jar <jar> server
```

## API

java-iceberg-toolkit provides APIs to perform operations on Iceberg tables and Hive tables. For Iceberg tables, Hive catalog is being used, but java-iceberg-toolkit will support other catalogs in the next releases.
<details><summary>Create an unpartitioned table:</summary>

```
import iceberg.IcebergConnector;

import org.apache.iceberg.PartitionSpec;
import org.apache.iceberg.Schema;
import org.apache.iceberg.types.Types;

IcebergConnector connector = new IcebergConnector(uri, warehouse, namespace, null);
Schema schema = new Schema(
            Types.NestedField.required(1, "ID", Types.IntegerType.get()),
            Types.NestedField.required(2, "Name", Types.StringType.get()),
            Types.NestedField.required(3, "Price", Types.DoubleType.get()),
            Types.NestedField.required(4, "Purchase_date", Types.TimestampType.withoutZone())
            )
        );
PartitionSpec spec = PartitionSpec.unpartitioned();
boolean overwrite = false;
connector.createTable(schema, spec, overwrite);
```

</details>

For a detailed list of API usage, please refer to [sample api usage](docs/sample_api_usage.md).

## Limitations

### Supported Types

The following table represents the list of Iceberg primitive data types supported by java-iceberg-toolkit:

No. | Primitive data type
---|---|
#1 | Types.IntegerType
#2 | Types.StringType
#3 | Types.DoubleType
#4 | Types.TimestampType (without zone)
#5 | Types.BinaryType
#6 | Types.BooleanType
#7 | Types.DateType
#8 | Types.TimestampType (with zone)
#9 | Types.DecimalType
#10 | Types.FixedType
#11 | Types.FloatType
#12 | Types.LongType
#13 | Types.TimeType
#14 | Types.UUIDType
