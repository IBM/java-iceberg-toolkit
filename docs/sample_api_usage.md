# API Usage

java-iceberg-toolkit provides APIs to perform operations on Iceberg tables and Hive tables. Majority of the operations on Hive tables are still being worked on, and this document will reflect any new features. 

**Table of Contents**

* [Hive Table Operations](/docs/sample_api_usage.md#hive-table-operations)
    1. [Table Information](/docs/sample_api_usage.md#table-information)

* [Iceberg Table Operations](/docs/sample_api_usage.md#iceberg-table-operations)
    1. [Create](/docs/sample_api_usage.md#create)
    2. [Write/Commit/Rewrite](/docs/sample_api_usage.md#writecommitrewrite)
    3. [Read](/docs/sample_api_usage.md#read)
    4. [Details](/docs/sample_api_usage.md#details)
    5. [Table Information](/docs/sample_api_usage.md#table-information-1)
    6. [Drop](/docs/sample_api_usage.md#drop)

## Hive Table Operations

### Table Information

* Get schema of a table
```
import iceberg.HiveConnector;

HiveConnector connector = new HiveConnector(uri, warehouse, namespace, table);
Schema schema = connector.getTableSchema();
```

* List plan files
```
import iceberg.HiveConnector;

import java.util.Map;

HiveConnector connector = new HiveConnector(uri, warehouse, namespace, table);
Map<Integer, List<Map<String, String>>> planFiles = connector.getPlanFiles();
```

## Iceberg Table Operations

### Create

* Create a partitioned table
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
PartitionSpec spec = PartitionSpec.builderFor(schema)
						.year("hour")
						.build();
boolean overwrite = false;
connector.createTable(schema, spec, overwrite);
```

* Create a namespace
```
import iceberg.IcebergConnector;

IcebergConnector connector = new IcebergConnector(uri, warehouse, namespace, null);
connector.createNamespace(Namespace.of(namespace));
```

### Write/Commit/Rewrite

* Write to a table and commit changes
```
import iceberg.IcebergConnector;

IcebergConnector connector = new IcebergConnector(uri, warehouse, namespace, table);

String record = '{"records":[{"ID":1,"Name":"Testing","Price": 1000,"Purchase_date":"2022-11-09T12:13:54.480"}]}';
String outputFile = null;
String dataFiles = connector.writeTable(record, outputFile);
connector.commitTable(dataFiles);
```

* Commit changes
```
import iceberg.IcebergConnector;

IcebergConnector connector = new IcebergConnector(uri, warehouse, namespace, table);
String dataFiles = '{"files":[{"file_path":"path_a"}]}';
connector.commitTable(dataFiles);
```

* Rewrite (replace) files containing same data
```
import iceberg.IcebergConnector;

IcebergConnector connector = new IcebergConnector(uri, warehouse, namespace, table);
String dataFiles = '{"files_to_del":[{"file_path":"path_a"}], "files_to_add":[{"file_path":"path_b"}]}';
connector.rewriteFiles(dataFiles);
```

### Read

* Read from a table
```
import iceberg.IcebergConnector;

import java.util.List;

IcebergConnector connector = new IcebergConnector(uri, warehouse, namespace, table);
List<List<String>> records = connector.readTable();
for (List<String> record : records) {
    for (int x = 0; x < record.size(); x++) {
        String comma = x == record.size() - 1 ? "" : ", ";
        System.out.print(record.get(x) + comma);
    }
    System.out.println();
}
```

### Details

* Get details of a namespace
```
import iceberg.IcebergConnector;

import java.util.Map;

IcebergConnector connector = new IcebergConnector(uri, warehouse, namespace, table);
Map<java.lang.String,java.lang.String> namespaceDetails = connector.loadNamespaceMetadata(Namespace.of(namespace));
```

* Get details of a table
```
import iceberg.IcebergConnector;

import org.apache.iceberg.Schema;

IcebergConnector connector = new IcebergConnector(uri, warehouse, namespace, table);
Snapshot snapshot = connector.getCurrentSnapshot();
Schema schema = connector.getTableSchema();
String location = connector.getTableLocation();
String dataLocation = connector.getTableDataLocation();
```

### Table Information

* List plan files
```
import iceberg.IcebergConnector;

import java.util.Map;

IcebergConnector connector = new IcebergConnector(uri, warehouse, namespace, table);
Map<Integer, List<Map<String, String>>> planFiles = connector.getPlanFiles();
```

* Get schema of a table
```
import iceberg.IcebergConnector;

import org.apache.iceberg.Schema;

IcebergConnector connector = new IcebergConnector(uri, warehouse, namespace, table);
Schema schema = connector.getTableSchema();
```

* Get uuid of a table
```
import iceberg.IcebergConnector;

IcebergConnector connector = new IcebergConnector(uri, warehouse, namespace, table);
String uuid = connector.getUUID();
```

* Get location of a table
```
import iceberg.IcebergConnector;

IcebergConnector connector = new IcebergConnector(uri, warehouse, namespace, table);
String location = connector.getTableLocation();
```

* Get current snapshot of a table
```
import iceberg.IcebergConnector;

import org.apache.iceberg.Snapshot;

IcebergConnector connector = new IcebergConnector(uri, warehouse, namespace, table);
Snapshot snapshot = connector.getCurrentSnapshot();
```

### Drop

* Drop a namespace
```
import iceberg.IcebergConnector;

IcebergConnector connector = new IcebergConnector(uri, warehouse, namespace, table);
connector.dropNamespace(Namespace.of(namespace));
```

* Drop a table
```
import iceberg.IcebergConnector;

IcebergConnector connector = new IcebergConnector(uri, warehouse, namespace, table);
Map<Integer, List<Map<String, String>>> planFiles = connector.getPlanFiles();
```