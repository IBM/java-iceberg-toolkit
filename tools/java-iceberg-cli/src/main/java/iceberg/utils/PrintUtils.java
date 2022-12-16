/**
 * (c) Copyright IBM Corp. 2022. All Rights Reserved.
 */

package iceberg.utils;

import iceberg.HiveConnector;
import iceberg.IcebergConnector;
import iceberg.MetastoreConnector;

import java.io.IOException;
import java.io.UnsupportedEncodingException;
import java.net.URISyntaxException;
import java.util.*;
import java.util.Map.Entry;
import iceberg.IcebergConnector.TableNotFoundException;

import org.apache.hadoop.hive.metastore.api.MetaException;
import org.apache.iceberg.DataFile;
import org.apache.iceberg.FileScanTask;
import org.apache.iceberg.Schema;
import org.apache.iceberg.SchemaParser;
import org.apache.iceberg.Snapshot;
import org.apache.iceberg.catalog.Namespace;
import org.apache.iceberg.catalog.TableIdentifier;
import org.apache.thrift.TException;
import org.apache.iceberg.PartitionSpec;
import org.json.JSONArray;
import org.json.JSONObject;

/**
 * 
 * Provides functions to output files, snapshot, schema, and table details
 * as a string, JSON, and CSV formats.
 *
 */
public class PrintUtils {
    /**
     * Get table details from MetastoreConnector and output to the user is the given format
     * @param metaConn
     * @param format
     * @throws URISyntaxException 
     * @throws IOException 
     */
    public static void printTableDetails(MetastoreConnector metaConn, String format) throws Exception, IOException, URISyntaxException {
        Map<Integer, List<Map<String, String>>> planFileTasks = metaConn.getPlanFiles();
        Snapshot snapshot = metaConn.getCurrentSnapshot();
        Schema targetSchema = metaConn.getTableSchema();
        String tableLocation = metaConn.getTableLocation();
        String dataLocation = metaConn.getTableDataLocation();
        switch (format) {
            case "json":
                getTableDetailsAsJson(planFileTasks, snapshot, targetSchema, tableLocation, dataLocation);
                break;
            case "csv":
                printTableDetailsAsCsv(planFileTasks, snapshot, targetSchema, tableLocation, dataLocation);
                break;
            default:
                printTableDetails(planFileTasks, snapshot, targetSchema, tableLocation, dataLocation);
        }
    }
    
    /**
     * Get list of tables from MetastoreConnector and output to the user is the given format
     * @param metaConn
     * @param format
     * @param namespace
     * @param uri
     * @param warehouse
     * @throws TException 
     */
    public static void printIcebergTables(MetastoreConnector metaConn, String format, String namespace, String uri, String warehouse) throws Exception {
        IcebergConnector metaConnIceberg = new IcebergConnector(uri, warehouse, namespace, null);
        java.util.List<String> tables_iceberg = metaConnIceberg.listTables(namespace);
        HiveConnector metaConnHive = new HiveConnector(uri, warehouse, namespace, null);
        java.util.List<String> tables_all = metaConnHive.listTables(namespace);
        ArrayList <String> table_ttype = new ArrayList<String>();
        for (String table : tables_all) {
            if (tables_iceberg.contains(namespace + "." + table)) {
                table_ttype.add(table.toString() + ": " + metaConnIceberg.getTableType(namespace, table));
            }
            else {
                table_ttype.add(table.toString() + ": " + metaConnHive.getTableType(namespace, table));
            }
        }
        switch (format) {
            case "json":
                listTablesAsJson(table_ttype, true);
                break;
            case "csv":
                listTablesAsCsv(table_ttype);
                break;
            default:
                printListOfTables(table_ttype);
        }
    }

    /**
     * Get list of namespaces from MetastoreConnector and output to the user in the given format
     * @param metaConn
     * @param format
     */
    public static void printNamespaces(MetastoreConnector metaConn, String format) throws Exception {
        java.util.List<Namespace> namespaces = metaConn.listNamespaces();
        ArrayList <String> namespace_location = new ArrayList<String>();
        for (Namespace nmspc : namespaces) {
            String location = metaConn.loadNamespaceMetadata(nmspc).get("location");
            String nmspc_str = nmspc.toString();
            namespace_location.add(nmspc_str + ": " + location);
        }
        switch (format) {
        case "json":
            listNamespacesAsJson(namespace_location, true);
            break;
        case "csv":
            listNamespacesAsCsv(namespace_location);
            break;
        default:
            printListOfNamespaces(namespace_location);
        }
    }
    
    /**
     * Get details of namespaces from MetastoreConnector and output to the user in the given format
     * @param metaConn
     * @param format
     * @param namespace
     */
    public static void printNamespaceDetails(MetastoreConnector metaConn, String format, String namespace) throws Exception {
        java.util.Map<java.lang.String,java.lang.String> details_nmspc = metaConn.loadNamespaceMetadata(Namespace.of(namespace));
        switch (format) {
        case "json":
            getNamespaceDetailsAsJson(details_nmspc, true);
            break;
        case "csv":
            printNamespaceDetailsAsCsv(details_nmspc);
            break;
        default:
            printDetailsOfNamespace(details_nmspc);
        }
    }
    
    /**
     * Get the table spec from MetastoreConnector and output to the user in the given format
     * @param metaConn
     */
    public static void printSpec(MetastoreConnector metaConn) throws Exception {
        PartitionSpec spec = metaConn.getSpec();
        String spec_details = spec.toString();
        System.out.println(spec_details);
    }
    
    /**
     * Get the table UUID from MetastoreConnector and output to the user in the given format
     * @param metaConn
     */
    public static void printUUID(MetastoreConnector metaConn) throws Exception {
        String uuid = metaConn.getUUID();
        System.out.println(uuid);
    }
    
    /**
     * Print details of a namespace as a JSON format
     * @param details
     * @param printObject
     * @return
     */
    private static JSONObject getNamespaceDetailsAsJson(java.util.Map<java.lang.String,java.lang.String> details, boolean printObject) {
        if (details.isEmpty()) {
            return null;    
        }
        JSONObject detailsJsonObject = new JSONObject(details);
        
        if (printObject) {
            System.out.println(detailsJsonObject);
        }
        
        return detailsJsonObject;
    }
    
    /**
     * Print details of a namespace in CSV format
     * @param details
     */
    private static void printNamespaceDetailsAsCsv(java.util.Map<java.lang.String,java.lang.String> details) {
        if (!details.isEmpty()) {    
            for (Map.Entry<String, String> entry : details.entrySet()) {
                System.out.println(entry.getKey() + ',' + entry.getValue());
              }
        }
    }
    
    /**
     * Print list of all namepsaces in a catalog
     * @param namespaces
     */
    private static void printDetailsOfNamespace(java.util.Map<java.lang.String,java.lang.String> details) {
        if (!details.isEmpty()) {
            System.out.println("DETAILS OF NAMESPACE");
            for (Entry<String, String> entry : details.entrySet()) {
                System.out.println(entry.getKey().toUpperCase() + ' ' + ':' + ' ' + entry.getValue());
            }
        }
    }
    
    /**
     * Get all table files from MetastoreConnector and output to the user is the given format
     * @param metaConn
     * @param format
     * @throws URISyntaxException 
     * @throws IOException 
     */
    public static void printFiles(MetastoreConnector metaConn, String format) throws IOException, URISyntaxException {
        Map<Integer, List<Map<String, String>>> planFiles = metaConn.getPlanFiles();
        switch (format) {
            case "json":
                getPlanFilesAsJSON(planFiles, true);
                break;
            case "csv":
                printPlanFiles(planFiles, ',');
                break;
            default:
                printPlanFiles(planFiles, ' ');
        }
    }
    
    /**
     * Get all table files from MetastoreConnector and output to the user is the given format
     * @param metaConn
     * @param format
     * @throws URISyntaxException 
     * @throws IOException 
     */
    public static void printFilesOld(MetastoreConnector metaConn, String format) throws IOException, URISyntaxException {
        Map<Integer, List<Map<String, String>>> planFiles = metaConn.getPlanFiles();
        switch (format) {
            case "json":
                oldGetPlanFilesAsJSON(planFiles, true);
                break;
            default:
                return;
        }
    }
    
    /**
     * Get all snapshots for a table from MetastoreConnector and output to the user is the given format
     * @param metaConn
     * @param format
     */
    public static void printSnapshots(MetastoreConnector metaConn, String format) throws Exception {
        java.lang.Iterable<Snapshot> snapshots = metaConn.getListOfSnapshots();
        switch (format) {
            case "json":
                getSnapshotsAsJson(snapshots, true);
                break;
            case "csv":
                printSnapshotsAsCsv(snapshots);
                break;
            default:
                printListOfSnapshots(snapshots);
        }
    }
    
    /**
     * Get default snapshot for a table from MetastoreConnector and output to the user is the given format
     * @param metaConn
     * @param format
     */
    public static void printCurrentSnapshot(MetastoreConnector metaConn, String format) throws Exception {
        Snapshot currentSnapshot = metaConn.getCurrentSnapshot();
        switch (format) {
            case "json":
                getCurrentSnapshotAsJson(currentSnapshot, true);
                break;
            case "csv":
                printCurrentSnapshotAsCsv(currentSnapshot);
                break;
            default:
                printCurrentSnapshot(currentSnapshot);
        }
    }
    
    /**
     * Get table schema from MetastoreConnector and output to the user is the given format
     * @param metaConn
     * @param format
     */
    public static void printSchema(MetastoreConnector metaConn, String format) {
        Schema tableSchema = metaConn.getTableSchema();
        switch (format) {
            case "json":
                getSchemaAsJson(tableSchema, true);
                break;
            case "csv":
                printSchemaAsCsv(tableSchema);
                break;
            default:
                printTableSchema(tableSchema);
        }
    }
    
    /**
     * Print csv record format
     * @param records
     */
    private static void printTableRecordsAsCsv(List<List<String>> records) {
        for (List<String> record : records) {
            for (int x = 0; x < record.size(); x++) {
                String comma = x == record.size() - 1 ? "" : ", ";
                System.out.print(record.get(x) + comma);
            }
            System.out.println();
        }
    }
    
    /**
     * Get table records from MetastoreConnector and output to the user is the given format
     * @param metaConn
     * @param format
     * @throws UnsupportedEncodingException 
     */
    public static void printTable(MetastoreConnector metaConn, String format) throws Exception, UnsupportedEncodingException {
        List<List<String>> records = metaConn.readTable();
        switch (format) {
        case "json":
            throw new UnsupportedOperationException("printtable json format not supported yet.");
        case "csv":
            printTableRecordsAsCsv(records);
            break;
        default:
            printTableRecordsAsCsv(records);
        }
    }
    
    /**
     * Print plan files in each task
     * @param planFileTasks
     */
    private static void printPlanFiles(Map<Integer, List<Map<String, String>>> planFileTasks, char delim) {
        if (planFileTasks != null) {
            System.out.println("TOTAL TASKS: " + planFileTasks.size());
            for (Map.Entry<Integer, List<Map<String, String>>> entry : planFileTasks.entrySet()) {
                System.out.println(String.format("TOTAL FILES IN TASK %d : %d", entry.getKey(),entry.getValue().size()));
                for (Map<String, String> task : entry.getValue()) {
                    String taskInfo = String.format("%s%c%s%c%s%c%s%c%s%c%s%c%s",
                                                    task.get("content"), delim,
                                                    task.get("file_path"), delim,
                                                    task.get("file_format"), delim,
                                                    task.get("start"), delim,
                                                    task.get("length"), delim,
                                                    task.get("spec"), delim,
                                                    task.get("residual")
                                                    );
                    System.out.println(taskInfo);
                }
            }
        }
    }
    
    /**
     * Print plan files in each task as a JSON format
     * @param planFileTasks
     * @param printObject
     * @return
     */
    private static JSONObject getPlanFilesAsJSON(Map<Integer, List<Map<String, String>>> planFileTasks, boolean printObject) {
        if (planFileTasks == null) {
            return null;
        }
        
        JSONObject planFilesJsonObject = new JSONObject();
        for (Map.Entry<Integer, List<Map<String, String>>> entry : planFileTasks.entrySet()) {
            JSONArray tasksArray = new JSONArray();
            for (Map<String, String> task : entry.getValue()) {
                JSONObject taskobj = new JSONObject();
                taskobj.put("content", task.get("content"));
                taskobj.put("file_path", task.get("file_path"));
                taskobj.put("file_format", task.get("file_format"));
                taskobj.put("start", task.get("start"));
                taskobj.put("length", task.get("length"));
                taskobj.put("spec", task.get("spec"));
                taskobj.put("residual", task.get("residual"));
                tasksArray.put(taskobj);
            }
            planFilesJsonObject.put(entry.getKey().toString(), tasksArray);
        }

        if (printObject) {
            System.out.println(planFilesJsonObject);
        }
        
        return planFilesJsonObject;
    }

    /**
     * Print plan files in each task as a JSON format
     * @param planFileTasks
     * @param printObject
     * @return
     */
    private static JSONObject oldGetPlanFilesAsJSON(Map<Integer, List<Map<String, String>>> planFileTasks, boolean printObject) {
        //THIS IS A HACK to return all files in one task for the current engine to adopt the latest iceberg_cli
        //this should be removed as soon as engine properly supports listfiles
        if (planFileTasks == null) {
            return null;
        }

        JSONObject planFilesJsonObject = new JSONObject();
        JSONArray tasksArray = new JSONArray();
        Map<String, Boolean> seen = new HashMap<String, Boolean>();
        for (Map.Entry<Integer, List<Map<String, String>>> entry : planFileTasks.entrySet()) {
            for (Map<String, String> task : entry.getValue()) {
                //hack! we don't want to repeat the same file twice, and the start/length are not currently
                //used in the engine
                if(!seen.containsKey(task.get("file_path"))) {
                    JSONObject taskobj = new JSONObject();
                    taskobj.put("file_path", task.get("file_path"));
                    tasksArray.put(taskobj);
                    seen.put(task.get("file_path"), true);
                }
            }
        }
        planFilesJsonObject.put("dataFiles", tasksArray);

        if (printObject) {
            System.out.println(planFilesJsonObject);
        }
        
        return planFilesJsonObject;
    }
    
    /**
     * Print latest snapshot
     * @param snapshot
     */
    private static void printCurrentSnapshot(Snapshot snapshot) {
        System.out.println("CURRENT SNAPSHOT");
        System.out.println(snapshot);
    }

    /**
     * Print most recent snapshot as a JSON object
     * @param snapshot
     * @param printObject
     * @return
     */
    private static JSONObject getCurrentSnapshotAsJson(Snapshot snapshot, boolean printObject) {
        JSONObject snapshotJsonObject = new JSONObject();

        if (snapshot == null) {
            return snapshotJsonObject.put("snapshot", new JSONObject());
        }
        
        snapshotJsonObject = DataConversion.snapshotToJson(snapshot);
        
        if (printObject) {
            System.out.println(snapshotJsonObject);
        }
        
        return snapshotJsonObject;
    }
    
    /**
     * Print most recent snapshot as comma separated values
     * @param snapshot
     */
    private static void printCurrentSnapshotAsCsv(Snapshot snapshot) {
        if (snapshot != null) {
            DataConversion.snapshotAsCsv(snapshot);
        }
    }
    
    /**
     * Print list of all snapshots
     * @param snapshots
     */
    private static void printListOfSnapshots(java.lang.Iterable<Snapshot> snapshots) {
        if (snapshots != null) {
            System.out.println("LIST OF SNAPSHOTS");
            for (Snapshot snapshot : snapshots)
                System.out.println(snapshot);
        }
    }
    
    /**
     * Print list of all snapshots in JSON format
     * @param snapshots
     * @param printObject
     * @return
     */
    private static JSONObject getSnapshotsAsJson(java.lang.Iterable<Snapshot> snapshots, boolean printObject) {
        if (snapshots == null) {
            return null;
        }
        
        JSONObject snapshotsJsonObject = DataConversion.snapshotsToJson(snapshots);
        
        if (printObject) {
            System.out.println(snapshotsJsonObject);
        }
        
        return snapshotsJsonObject;
    }

    /**
     * Print list of all snapshots in CSV format
     * @param snapshots
     */
    private static void printSnapshotsAsCsv(java.lang.Iterable<Snapshot> snapshots) {
        DataConversion.snapshotsAsCsv(snapshots);
    }

    /**
     * Print scan task files, most recent snapshot, and schema
     * @param planFileTasks
     * @param snapshot
     * @param schema
     */
    private static void printTableDetails(Map<Integer, List<Map<String, String>>> planFileTasks, Snapshot snapshot, Schema schema, String tableLocation, String dataLocation) {
        printPlanFiles(planFileTasks, ' ');
        System.out.println();
        printCurrentSnapshot(snapshot);
        System.out.println();
        printTableSchema(schema);
        System.out.println();
        System.out.println("TABLE LOCATION");
        System.out.println(tableLocation);
        System.out.println();
        System.out.println("DATA LOCATION");
        System.out.println(dataLocation);
    }
    
    /**
     * Print scan task files, most recent snapshot, and schema in JSON format
     * @param planFileTasks
     * @param snapshot
     * @param schema
     * @return
     */
    private static JSONObject getTableDetailsAsJson(Map<Integer, List<Map<String, String>>> planFileTasks, Snapshot snapshot, Schema schema, String tableLocation, String dataLocation) {
        JSONObject tableDetails = new JSONObject();
        
        JSONObject dataFiles = getPlanFilesAsJSON(planFileTasks, false);
        JSONObject schemaObject = getSchemaAsJson(schema, false);
        JSONObject currentSnapshot = getCurrentSnapshotAsJson(snapshot, false);
        tableDetails.put("files", dataFiles);
        tableDetails.put("snapshot", currentSnapshot.get("snapshot"));
        tableDetails.put("schema", schemaObject.get("schema"));
        tableDetails.put("tableLocation", tableLocation);
        tableDetails.put("dataLocation", dataLocation);
        
        System.out.println(tableDetails);
        
        return tableDetails;
    }

    /**
     * Print scan task files, most recent snapshot, and schema in CSV format
     * @param planFileTasks
     * @param snapshot
     * @param schema
     */
    private static void printTableDetailsAsCsv(Map<Integer, List<Map<String, String>>> planFileTasks, Snapshot snapshot, Schema schema, String tableLocation, String dataLocation) {
        printPlanFiles(planFileTasks, ',');
        System.out.println();
        System.out.println("SNAPSHOT");
        printCurrentSnapshotAsCsv(snapshot);
        System.out.println();
        System.out.println("SCHEMA");
        printSchemaAsCsv(schema);
        System.out.println();
        System.out.println("TABLE LOCATION");
        System.out.println(tableLocation);
        System.out.println();
        System.out.println("DATA LOCATION");
        System.out.println(dataLocation);
    }
    
    /**
     * Print table schema
     * @param schema
     */
    private static void printTableSchema(Schema schema) {
        System.out.println("SCHEMA");
        System.out.println(schema);
    }
    
    /**
     * Print table schema in JSON format
     * @param schema
     * @param printObject
     * @return
     */
    private static JSONObject getSchemaAsJson(Schema schema, boolean printObject) {
        JSONObject schemaObject = new JSONObject();
        
        String schemaAsJson = (schema == null) ? "{}" : SchemaParser.toJson(schema);
        schemaObject.put("schema", new JSONObject(schemaAsJson));

        if (printObject) {
            System.out.println(schemaObject);
        }

        return schemaObject;
    }
    
    /**
     * Print table schema in CSV format
     * @param schema
     */
    private static void printSchemaAsCsv(Schema schema) {
        DataConversion.schemaAsCsv(schema);
    }

    /**
     * Print list of all tables in a namespace
     * @param tables
     */
    private static void printListOfTables(java.util.List<String> tables) {
        if (tables != null) {
            System.out.println("LIST OF TABLES");
            System.out.println("TABLE NAME: TABLE TYPE");
            for (String table : tables)
                System.out.println(table);
        }
    }
    
    /**
     * Print list of all namepsaces in a catalog
     * @param namespaces
     */
    private static void printListOfNamespaces(ArrayList <String> namespace_location) {
        if (namespace_location != null) {
            System.out.println("LIST OF NAMESPACES");
            System.out.println("NAMESPACE: LOCATION");
            for (String nmspc : namespace_location)
                System.out.println(nmspc);
        }
    }
    
    /**
     * Print list of all tables in a namespace in JSON format
     * @param tables
     * @param printObject
     * @return
     */
    private static JSONObject listTablesAsJson(java.util.List<String> tables, boolean printObject) {
        if (tables == null) {
            return null;
        }
        
        JSONObject tablesJsonObject = new JSONObject();
        JSONArray listOfTables = new JSONArray();
        for (String table : tables)
            listOfTables.put(table);

        tablesJsonObject.put("table:type", listOfTables);
        
        if (printObject) {
            System.out.println(tablesJsonObject);
        }
        
        return tablesJsonObject;
    }

    /**
     * Print list of all namepsaces in a catalog in JSON format
     * @param namespaces
     * @param printObject
     * @return
     */
    private static JSONObject listNamespacesAsJson(ArrayList <String> namespace_location, boolean printObject) {
        if (namespace_location == null) {
            return null;
        }
        
        JSONObject nmspcJsonObject = new JSONObject();
        JSONArray listOfNamespaces = new JSONArray();
        for (String nmspc : namespace_location)
            listOfNamespaces.put(nmspc);

        nmspcJsonObject.put("namespace:location", listOfNamespaces);
        
        if (printObject) {
            System.out.println(nmspcJsonObject);
        }
        
        return nmspcJsonObject;
    }    
    
    
    /**
     * Print list of all tables in a namespace in CSV format
     * @param tables
     */
    private static void listTablesAsCsv(java.util.List<String> tables) {
        if (tables != null) {
            for (String table : tables)
                System.out.println(table);
        }
    }
    
    /**
     * Print list of all namepsaces in a catalog in CSV format
     * @param namespaces
     */
    private static void listNamespacesAsCsv(ArrayList <String>  namespace_location) {
        if (namespace_location != null) {
            for (String nmspc : namespace_location)
                System.out.print(nmspc + ", ");
        }
    }

}