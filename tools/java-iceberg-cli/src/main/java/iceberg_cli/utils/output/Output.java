/**
 * (c) Copyright IBM Corp. 2022. All Rights Reserved.
 */

package iceberg_cli.utils.output;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;

import org.apache.iceberg.Schema;
import org.apache.iceberg.Snapshot;

public class Output {
    
    public String tableMetadata(Snapshot snapshot, Schema schema, String tableLocation, String dataLocation, String type) throws Exception {
        StringBuilder builder = new StringBuilder();
        builder.append("SNAPSHOT\n");
        builder.append(currentSnapshot(snapshot));
        builder.append("\nSCHEMA\n");
        builder.append(tableSchema(schema));
        builder.append("\nTABLE LOCATION\n");
        builder.append(tableLocation);
        builder.append("\nDATA LOCATION\n");
        builder.append(dataLocation);
        builder.append("\nTABLE TYPE\n");
        builder.append(type);
        
        return builder.toString();
    }
    
    public String tableDetails(Map<Integer, List<Map<String, String>>> planFileTasks, Snapshot snapshot,
            Schema schema, String tableLocation, String dataLocation, String type) throws Exception {
        StringBuilder builder = new StringBuilder();
        builder.append(tableFiles(planFileTasks));
        builder.append("SNAPSHOT\n");
        builder.append(currentSnapshot(snapshot));
        builder.append("\nSCHEMA\n");
        builder.append(tableSchema(schema));
        builder.append("\nTABLE LOCATION\n");
        builder.append(tableLocation);
        builder.append("\nDATA LOCATION\n");
        builder.append(dataLocation);
        builder.append("\nTABLE TYPE\n");
        builder.append(type);
        
        return builder.toString();
    }
    
    public String listAllTables(Map<String, List<String>> tables) throws Exception {
        StringBuilder builder = new StringBuilder();
        if (tables != null) {
            for (Entry <String, List<String>> entry : tables.entrySet()) {
                builder.append(entry.getKey().toUpperCase() + ":\n");
                for (String table : entry.getValue())
                    builder.append(String.format("%s\n", table));
            }
        }
        return builder.toString();
    }
    
    public String listTables(List<String> tables) throws Exception {
        StringBuilder builder = new StringBuilder();
        if (tables != null) {
            builder.append("LIST OF TABLES\n");
            builder.append("TABLE NAME\n");
            for (String table : tables)
                builder.append(String.format("%s\n", table));
        }
        return builder.toString();
    }
    
    public String listNamespaces(ArrayList<String> namespace_location) throws Exception {
        StringBuilder builder = new StringBuilder();
        if (namespace_location != null) {
            builder.append("LIST OF NAMESPACES\n");
            builder.append("NAMESPACE: LOCATION\n");
            for (String nmspc : namespace_location)
                builder.append(String.format("%s\n", nmspc));
        }
        return builder.toString();
    }
    
    public String namespaceDetails(Map<String,String> details) throws Exception {
        StringBuilder builder = new StringBuilder();
        if (!details.isEmpty()) {
            builder.append("DETAILS OF NAMESPACE\n");
            for (Entry<String, String> entry : details.entrySet()) {
                builder.append(entry.getKey().toUpperCase() + ' ' + ':' + ' ' + entry.getValue());
                builder.append("\n");
            }
        }
        return builder.toString();
    }
    
    public String tableFiles(Map<Integer, List<Map<String, String>>> planFileTasks) throws Exception {
        StringBuilder builder = new StringBuilder();
        
        // Add data files
        char delim = ' ';
        if (planFileTasks != null) {
            builder.append(String.format("TOTAL TASKS : %d\n", planFileTasks.size()));
            for (Map.Entry<Integer, List<Map<String, String>>> entry : planFileTasks.entrySet()) {
                builder.append(String.format("TOTAL FILES IN TASK %d : %d\n", entry.getKey(),entry.getValue().size()));
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
                    builder.append(String.format("%s\n", taskInfo));
                }
            }
        }
        
        return builder.toString();
    }
    
    public String allSnapshots(java.lang.Iterable<Snapshot> snapshots) throws Exception {
        StringBuilder builder = new StringBuilder();
        if (snapshots != null) {
            builder.append("LIST OF SNAPSHOTS\n");
            for (Snapshot snapshot : snapshots)
                builder.append(String.format("%s\n", snapshot));
        }
        return builder.toString();
    }
    
    public String currentSnapshot(Snapshot snapshot) throws Exception {
        if (snapshot != null)
            return snapshot.toString();
        
        return null;
    }
    
    public String tableSchema(Schema schema) throws Exception {
        if (schema != null)
            return schema.toString();
        
        return null;
    }
    
    public String tableRecords(List<List<String>> records) throws Exception {
        StringBuilder builder = new StringBuilder();
        for (List<String> record : records) {
            for (int x = 0; x < record.size(); x++) {
                String comma = x == record.size() - 1 ? "" : ", ";
                builder.append(record.get(x) + comma);
            }
            builder.append("\n");
        }
        return builder.toString();
    }
}
