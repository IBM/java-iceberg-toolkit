/**
 * (c) Copyright IBM Corp. 2022. All Rights Reserved.
 */

package iceberg_cli.utils.output;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.iceberg.Schema;
import org.apache.iceberg.SchemaParser;
import org.apache.iceberg.Snapshot;
import org.json.JSONArray;
import org.json.JSONObject;

import iceberg_cli.utils.DataConversion;

public class JsonOutput extends Output{
    
    @Override
    public String tableMetadata(Snapshot snapshot, Schema schema, String tableLocation, String dataLocation, String type) throws Exception {
        JSONObject tableMetadata = new JSONObject();
        
        JSONObject schemaObject = new JSONObject(tableSchema(schema));
        JSONObject currentSnapshot = new JSONObject(currentSnapshot(snapshot));
        tableMetadata.put("snapshot", currentSnapshot.get("snapshot"));
        tableMetadata.put("schema", schemaObject.get("schema"));
        tableMetadata.put("tableLocation", tableLocation);
        tableMetadata.put("dataLocation", dataLocation);
        tableMetadata.put("tableType", type);
                
        return tableMetadata.toString();
    }

    @Override
    public String tableDetails(Map<Integer, List<Map<String, String>>> planFileTasks, Snapshot snapshot,
            Schema schema, String tableLocation, String dataLocation, String type) throws Exception {
        JSONObject tableDetails = new JSONObject();
        
        JSONObject dataFiles = new JSONObject(tableFiles(planFileTasks));
        JSONObject schemaObject = new JSONObject(tableSchema(schema));
        JSONObject currentSnapshot = new JSONObject(currentSnapshot(snapshot));
        tableDetails.put("files", dataFiles);
        tableDetails.put("snapshot", currentSnapshot.get("snapshot"));
        tableDetails.put("schema", schemaObject.get("schema"));
        tableDetails.put("tableLocation", tableLocation);
        tableDetails.put("dataLocation", dataLocation);
        tableDetails.put("tableType", type);
                
        return tableDetails.toString();
    }

    @Override
    public String listTables(List<String> tables) throws Exception {
        if (tables == null) {
            return null;
        }
        
        JSONObject tablesJsonObject = new JSONObject();
        JSONArray listOfTables = new JSONArray();
        for (String table : tables)
            listOfTables.put(table);

        tablesJsonObject.put("table", listOfTables);
        
        return tablesJsonObject.toString();
    }

    @Override
    public String listNamespaces(ArrayList<String> namespace_location) throws Exception {
        if (namespace_location == null) {
            return null;
        }
        
        JSONObject nmspcJsonObject = new JSONObject();
        JSONArray listOfNamespaces = new JSONArray();
        for (String nmspc : namespace_location)
            listOfNamespaces.put(nmspc);

        nmspcJsonObject.put("namespace:location", listOfNamespaces);
        
        return nmspcJsonObject.toString();
    }

    @Override
    public String namespaceDetails(Map<String,String> details) throws Exception {
        if (details.isEmpty()) {
            return null;    
        }
        JSONObject detailsJsonObject = new JSONObject(details);
        
        return detailsJsonObject.toString();
    }

    @Override
    public String tableFiles(Map<Integer, List<Map<String, String>>> planFileTasks) throws Exception {
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
        
        return planFilesJsonObject.toString();
    }

    @Override
    public String allSnapshots(java.lang.Iterable<Snapshot> snapshots) throws Exception {
        if (snapshots == null) {
            return null;
        }
        
        JSONObject snapshotsJsonObject = DataConversion.snapshotsToJson(snapshots);
        
        return snapshotsJsonObject.toString();
    }

    @Override
    public String currentSnapshot(Snapshot snapshot) throws Exception {
        JSONObject snapshotJsonObject = new JSONObject();

        if (snapshot == null) {
            snapshotJsonObject.put("snapshot", new JSONObject());
        } else {
            snapshotJsonObject = DataConversion.snapshotToJson(snapshot);
        }
        
        return snapshotJsonObject.toString();
    }

    @Override
    public String tableSchema(Schema schema) throws Exception {
        JSONObject schemaObject = new JSONObject();
        
        String schemaAsJson = (schema == null) ? "{}" : SchemaParser.toJson(schema);
        schemaObject.put("schema", new JSONObject(schemaAsJson));

        return schemaObject.toString();
    }
    
    @Override
    public String tableRecords(List<List<String>> records) throws Exception {
        throw new UnsupportedOperationException("Displaying table records in json format is not supported yet.");
    }
}
