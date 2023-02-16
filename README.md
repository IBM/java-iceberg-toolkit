# java-iceberg-toolkit
java-iceberg-toolkit is a Java implementation for performing operations on Apache Iceberg and Hive tables. For a detailed list of supported operations, refer to [Supported Operations](/README.md#supported-operations).

**Table of Contents**
* [Pre-Requisites](/README.md#pre-requisites)
* [Limitations](/README.md#limitations)
* [Install and Build](/README.md#install-and-build)
* [Configuration](/README.md#configuration)
* [CLI](/README.md#cli-2)
* [API](/README.md#api)


## Pre-Requisites

### CLI

1. Java version 8 and above
2. Apache Maven Dependencies 
3. AWS Credentials set as environment variables 

### Testing

1. Java version 13 and above
2. JUnit5
3. AWS Credentials set as environment variable
4. URI set as "URI" environment variable
5. Warehouse set as "WAREHOUSE" environment variable

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

## Install and Build 

1- Build an image
```
sudo ./createImages.sh
```

2- Create a container and run it as a detached process
```
sudo podman run -d localhost/java-iceberg-cli:latest
```

3- Open a remote shell to the container
```
sudo podman exec -it <container_name> bash
```

4- Run the Java tool
```
java -jar /home/java-iceberg-cli/target/<jar_name> --help
```

## Configuration

### CLI
Pass in the URI to the Hive Metastore using -u or --uri options. You would need to pass the warehouse path when creating a new namespace using -w or --warehouse options. 

Set credentials using environment variables as:
```
export AWS_ACCESS_KEY_ID=
export AWS_SECRET_ACCESS_KEY=
export AWS_REGION=
```

### Unit Tests
Set credentials using environment variables as:
```
export AWS_ACCESS_KEY_ID=
export AWS_SECRET_ACCESS_KEY=
export AWS_REGION=
export URI=
export WAREHOUSE=
```
If URI and WAREHOUSE are not set as environment variables, the test will promp you to enter them using the command line before the tests are run. 

## CLI

java-iceberg-toolkit comes with a CLI which is ready to use when the code is packaged and all configurations are in place.

### Quickstart

The CLI provides various operations on Iceberg and Hive tables.
```
$ java -jar <jar_name> --help
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
Describe a namespace or a table | Y
Drop a namespace or a table | Y 
Get plan tasks of a table | Y | Y
Get schema of a table | Y | Y
Get uuid of a table | Y |
Get partition spec of a table | Y |
Get current or all snapshots of a table | Y |
List namespaces | Y | Y
List tables in a namespace | Y | Y
Read from a table | Y
Rename a table | Y |
Write to a table | Y |

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
