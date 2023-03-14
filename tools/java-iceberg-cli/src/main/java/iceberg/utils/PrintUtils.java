/**
 * (c) Copyright IBM Corp. 2023. All Rights Reserved.
 */

package iceberg.utils;

import iceberg.HiveConnector;
import iceberg.IcebergConnector;
import iceberg.MetastoreConnector;
import iceberg.utils.output.*;

import java.util.*;

import org.apache.iceberg.Schema;
import org.apache.iceberg.Snapshot;
import org.apache.iceberg.catalog.Namespace;
import org.apache.iceberg.PartitionSpec;
import org.json.JSONObject;

/**
 * 
 * Provides functions to output files, snapshot, schema, and table details
 * as a string, JSON, and CSV formats.
 *
 */
public class PrintUtils {
    private MetastoreConnector metaConn;
    private String format;
    private Output output;
    
    public PrintUtils(MetastoreConnector metaConn, String format) {
        this.metaConn = metaConn;
        this.format = format;
        switch (format) {
            case "json":
                output = new JsonOutput();
                break;
            case "csv":
                output = new CsvOutput();
                break;
            default:
                output = new Output();
        }
    }
    
    /**
     * Get table details from MetastoreConnector and output to the user is the given format
     * @throws Exception 
     */
    public String printTableDetails() throws Exception {
        Map<Integer, List<Map<String, String>>> planFileTasks = metaConn.getPlanFiles();
        Snapshot snapshot = metaConn.getCurrentSnapshot();
        Schema targetSchema = metaConn.getTableSchema();
        String tableLocation = metaConn.getTableLocation();
        String dataLocation = metaConn.getTableDataLocation();
        
        return output.tableDetails(planFileTasks, snapshot, targetSchema, tableLocation, dataLocation);
    }
    
    /**
     * Get list of tables from MetastoreConnector and output to the user is the given format
     * @param namespace
     * @param uri
     * @param warehouse
     * @throws Exception 
     */
    public String printTables(String namespace, String uri, String warehouse) throws Exception {
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
        
        return output.listTables(table_ttype);
    }

    /**
     * Get list of namespaces from MetastoreConnector and output to the user in the given format
     */
    public String printNamespaces() throws Exception {
        java.util.List<Namespace> namespaces = metaConn.listNamespaces();
        ArrayList <String> namespace_location = new ArrayList<String>();
        for (Namespace nmspc : namespaces) {
            String location = metaConn.loadNamespaceMetadata(nmspc).get("location");
            String nmspc_str = nmspc.toString();
            namespace_location.add(nmspc_str + ": " + location);
        }
        return output.listNamespaces(namespace_location);
    }
    
    /**
     * Get details of namespaces from MetastoreConnector and output to the user in the given format
     * @param namespace
     */
    public String printNamespaceDetails(String namespace) throws Exception {
        java.util.Map<java.lang.String,java.lang.String> details_nmspc = metaConn.loadNamespaceMetadata(Namespace.of(namespace));
        return output.namespaceDetails(details_nmspc);
    }
    
    /**
     * Get the table spec from MetastoreConnector and output to the user in the given format
     */
    public String printSpec() throws Exception {
        PartitionSpec spec = metaConn.getSpec();
        return spec.toString();
    }
    
    /**
     * Get the table UUID from MetastoreConnector and output to the user in the given format
     */
    public String printUUID() throws Exception {
        return metaConn.getUUID();
    }
    
    /**
     * Get all table files from MetastoreConnector and output to the user is the given format
     * @throws Exception 
     */
    public String printFiles() throws Exception {
        String outputString = null;
        
        String planFiles = output.tableFiles(metaConn.getPlanFiles());
        Long snapshotId = metaConn.getCurrentSnapshotId();
        switch (format) {
            case "json":
                JSONObject filesAsJson = new JSONObject(planFiles);
                filesAsJson.put("snaphotId", snapshotId);
                outputString = filesAsJson.toString();
                break;
            default:
                StringBuilder builder = new StringBuilder(String.format("SNAPSHOT ID : %d\n", snapshotId));
                builder.append(planFiles);
                outputString = builder.toString();
        }
        return outputString;
    }
    
    /**
     * Get all snapshots for a table from MetastoreConnector and output to the user is the given format
     */
    public String printSnapshots() throws Exception {
        java.lang.Iterable<Snapshot> snapshots = metaConn.getListOfSnapshots();
        return output.allSnapshots(snapshots);
    }
    
    /**
     * Get default snapshot for a table from MetastoreConnector and output to the user is the given format
     */
    public String printCurrentSnapshot() throws Exception {
        Snapshot currentSnapshot = metaConn.getCurrentSnapshot();
        return output.currentSnapshot(currentSnapshot);
    }
    
    /**
     * Get table schema from MetastoreConnector and output to the user is the given format
     * @throws Exception
     */
    public String printSchema() throws Exception {
        String outputString = null;
        
        String schema = output.tableSchema(metaConn.getTableSchema());
        Long snapshotId = metaConn.getCurrentSnapshotId();
        switch (format) {
            case "json":
                JSONObject schemaAsJson = new JSONObject(schema);
                schemaAsJson.put("snaphotId", snapshotId);
                outputString = schemaAsJson.toString();
                break;
            default:
                StringBuilder builder = new StringBuilder(String.format("SNAPSHOT ID : %d\n", snapshotId));
                builder.append("SCHEMA\n");
                builder.append(schema);
                outputString = builder.toString();
        }
        return outputString;
    }
    
    /**
     * Get table records from MetastoreConnector and output to the user is the given format
     * @throws Exception 
     */
    public String printTable() throws Exception {
        List<List<String>> records = metaConn.readTable();
        return output.tableRecords(records);
    }
}
