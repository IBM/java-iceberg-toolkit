/**
 * (c) Copyright IBM Corp. 2023. All Rights Reserved.
 */

package iceberg.utils.output;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import org.apache.iceberg.Schema;
import org.apache.iceberg.Snapshot;

import iceberg.utils.DataConversion;

public class CsvOutput extends Output {

    @Override
    public String tableDetails(Map<Integer, List<Map<String, String>>> planFileTasks, Snapshot snapshot,
            Schema schema, String tableLocation, String dataLocation) throws Exception {
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
        
        return builder.toString();
    }

    @Override
    public String listTables(List<String> tables) throws Exception {
        StringBuilder builder = new StringBuilder();
        if (tables != null) {
            for (String table : tables)
                builder.append(String.format("%s\n", table));
        }
        return builder.toString();
    }

    @Override
    public String listNamespaces(ArrayList<String> namespace_location) throws Exception {
        StringBuilder builder = new StringBuilder();
        if (namespace_location != null) {
            for (String nmspc : namespace_location)
                builder.append(String.format("%s,\n", nmspc));
        }
        return builder.toString();
    }

    @Override
    public String namespaceDetails(Map<String,String> details) throws Exception {
        StringBuilder builder = new StringBuilder();
        if (!details.isEmpty()) {    
            for (Map.Entry<String, String> entry : details.entrySet()) {
                builder.append(entry.getKey() + ',' + entry.getValue());
                builder.append("\n");
            }
        }
        return builder.toString();
    }

    @Override
    public String tableFiles(Map<Integer, List<Map<String, String>>> planFileTasks) throws Exception {
        StringBuilder builder = new StringBuilder();
        
        // Add data files
        char delim = ',';
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

    @Override
    public String allSnapshots(java.lang.Iterable<Snapshot> snapshots) throws Exception {
        return DataConversion.snapshotsAsCsv(snapshots);
    }

    @Override
    public String currentSnapshot(Snapshot snapshot) throws Exception {
        if (snapshot != null)
            return DataConversion.snapshotAsCsv(snapshot);
        
        return null;
    }

    @Override
    public String tableSchema(Schema schema) throws Exception {
        return DataConversion.schemaAsCsv(schema).trim();
    }
}