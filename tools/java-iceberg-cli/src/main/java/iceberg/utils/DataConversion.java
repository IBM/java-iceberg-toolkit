/**
 * (c) Copyright IBM Corp. 2022. All Rights Reserved.
 */

package iceberg.utils;

import java.io.UnsupportedEncodingException;
import java.math.BigDecimal;
import java.nio.ByteBuffer;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.LocalTime;
import java.time.OffsetDateTime;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.UUID;

import org.apache.iceberg.DataFile;
import org.apache.iceberg.FileScanTask;
import org.apache.iceberg.Schema;
import org.apache.iceberg.Snapshot;
import org.apache.iceberg.types.Type;
import org.apache.iceberg.types.Types;
import org.json.JSONArray;
import org.json.JSONObject;

/**
 * 
 * Provides data conversion functions to and from Iceberg data types
 *
 */
public class DataConversion 
{
    /**
     * 
     * @param value
     * @param colType
     * @return Object representing the value as an Iceberg data type
     * @throws UnsupportedEncodingException
     */
    public static Object stringToIcebergType(String value, Type colType) throws UnsupportedEncodingException {
        if (value.equalsIgnoreCase("null")) 
            return null;
        
        // TODO: Add support for nested types
        switch (colType.typeId()) {
            case BOOLEAN:
                return value.equalsIgnoreCase("true");
            case INTEGER:
                return Integer.parseInt(value);
            case LONG:
                return Long.parseLong(value);
            case FLOAT:
                return Float.parseFloat(value);
            case DOUBLE:
                return Double.parseDouble(value);
            case STRING:
                return value;
            case UUID:
                UUID uuid = UUID.fromString(value);
                ByteBuffer buffer = ByteBuffer.wrap(new byte[16]);
                buffer.putLong(uuid.getMostSignificantBits());
                buffer.putLong(uuid.getLeastSignificantBits());
                return buffer.array();
            case FIXED:
                Types.FixedType fixed = (Types.FixedType) colType;
                byte[] fixedValue = Arrays.copyOf(value.getBytes("UTF-8"), fixed.length());
                return fixedValue;
            case BINARY:
                byte[] binaryValue = value.getBytes("UTF-8");
                return ByteBuffer.wrap(binaryValue);
            case DECIMAL:
                return new BigDecimal(value);
            case DATE:
                return LocalDate.parse(value);
            case TIME:
                return LocalTime.parse(value);
            case TIMESTAMP:
                if (colType == Types.TimestampType.withZone()) {
                    return OffsetDateTime.parse(value);
                } else {
                    return LocalDateTime.parse(value);
                }
            default:
                throw new IllegalArgumentException("Unsupported column type");
        }
    }
    
    /**
     * 
     * @param planFiles
     * @return JSONObject containing the planFiles information
     */
    public static JSONObject dataFilesToJson(Map<Integer, List<FileScanTask>> planFileTasks) {
        JSONObject filesPerTask = new JSONObject();
        for (Map.Entry<Integer, List<FileScanTask>> entry : planFileTasks.entrySet()) {
            JSONArray listOfFiles = new JSONArray();
            for (FileScanTask task : entry.getValue()) {
                JSONObject fileInfo = new JSONObject();
                DataFile file = task.file();
                fileInfo.put("content", file.content());
                fileInfo.put("file_path", file.path());
                fileInfo.put("file_format", file.format());
                fileInfo.put("start", task.start());
                fileInfo.put("length", task.length());
                fileInfo.put("spec", task.spec());
                fileInfo.put("residual", task.residual());
                
                listOfFiles.put(fileInfo);
            }
            filesPerTask.put(entry.getKey().toString(), listOfFiles);
        }
        
        return filesPerTask;
    }
    
    /**
     * 
     * @param file
     * @return JSONObject representing the data file's metadata
     */
    public static JSONObject dataFileToJson(DataFile file) {
        JSONObject fileInfo = new JSONObject();
        fileInfo.put("content", file.content());
        fileInfo.put("file_path", file.path());
        fileInfo.put("file_format", file.format());
        fileInfo.put("spec_id", file.specId());
        fileInfo.put("partition", file.partition());
        fileInfo.put("record_count", file.recordCount());
        fileInfo.put("file_size_in_bytes", file.fileSizeInBytes());
        fileInfo.put("column_sizes", file.columnSizes());
        fileInfo.put("value_counts", file.valueCounts());
        fileInfo.put("null_value_counts", file.nullValueCounts());
        fileInfo.put("nan_value_counts", file.nanValueCounts());
        fileInfo.put("lower_bounds", file.lowerBounds());
        fileInfo.put("upper_bounds", file.upperBounds());
        fileInfo.put("key_metadata", file.keyMetadata());
        fileInfo.put("split_offsets", file.splitOffsets());
        fileInfo.put("equality_ids", file.equalityFieldIds());
        fileInfo.put("sort_order_id", file.sortOrderId());
        
        return fileInfo;
    }
    
    /**
     * 
     * @param snapshot
     * @return JSONObject containing the snapshot information
     */
    public static JSONObject snapshotToJson(Snapshot snapshot) {
        JSONObject snapshotJsonObject = new JSONObject();
        JSONObject snapshotInfo = new JSONObject();
        snapshotInfo.put("id", snapshot.snapshotId());
        snapshotInfo.put("timestamp", snapshot.timestampMillis());
        snapshotInfo.put("operation", snapshot.operation());
        snapshotInfo.put("summary", snapshot.summary());
        snapshotInfo.put("manifest-list", snapshot.manifestListLocation());
        snapshotInfo.put("schema-id", snapshot.schemaId());

        snapshotJsonObject.put("snapshot", snapshotInfo);
        
        return snapshotJsonObject;
    }
    
    /**
     * 
     * @param snapshots
     * @return JSONObject containing the snapshots information
     */
    public static JSONObject snapshotsToJson(java.lang.Iterable<Snapshot> snapshots) {
        JSONObject snapshotsJsonObject = new JSONObject();
        JSONArray listOfSnapshots = new JSONArray();
        for (Snapshot snapshot : snapshots) {
            JSONObject snapshotInfo = new JSONObject();
            snapshotInfo.put("id", snapshot.snapshotId());
            snapshotInfo.put("timestamp", snapshot.timestampMillis());
            snapshotInfo.put("operation", snapshot.operation());
            snapshotInfo.put("summary", snapshot.summary());
            snapshotInfo.put("manifest-list", snapshot.manifestListLocation());
            snapshotInfo.put("schema-id", snapshot.schemaId());
            
            listOfSnapshots.put(snapshotInfo);
        }

        snapshotsJsonObject.put("snapshots", listOfSnapshots);
        
        return snapshotsJsonObject;
    }
    
    /**
     * Represent metadata of plan files as comma delimited values
     * @param planFileTasks
     */
    public static void dataFilesAsCsv(Map<Integer, List<FileScanTask>> planFileTasks) {
        System.out.println("TOTAL TASKS: " + planFileTasks.size());
        for (Map.Entry<Integer, List<FileScanTask>> entry : planFileTasks.entrySet()) {
            System.out.println(String.format("TOTAL FILES IN TASK %d : %d", entry.getKey(),entry.getValue().size()));
            for (FileScanTask task : entry.getValue()) {
                DataFile file = task.file();
                String taskInfo = String.format("%s,%s,%s,%d,%d,%s,%s", 
                                                file.content(),
                                                file.path(),
                                                file.format(),
                                                task.start(),
                                                task.length(),
                                                task.spec(),
                                                task.residual()
                                                );
                System.out.println(taskInfo);
            }
        }
    }
    
    /**
     * Represent a snapshot metadata as comma delimited values
     * @param snapshot
     */
    public static void snapshotAsCsv(Snapshot snapshot) {
        String snapshotInfo = 
            String.format("%d,%d,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%d",
                        snapshot.snapshotId(),
                        snapshot.timestampMillis(),
                        snapshot.operation(),
                        snapshot.summary().get("added-data-files"),
                        snapshot.summary().get("added-records"),
                        snapshot.summary().get("added-files-size"),
                        snapshot.summary().get("changed-partition-count"),
                        snapshot.summary().get("total-records"),
                        snapshot.summary().get("total-data-files"),
                        snapshot.summary().get("total-delete-files"),
                        snapshot.summary().get("total-position-deletes"),
                        snapshot.summary().get("total-equality-deletes"),
                        snapshot.manifestListLocation(),
                        snapshot.schemaId()
                        );
        System.out.println(snapshotInfo);
    }
    
    /**
     * Represent snapshots information as comma delimited values
     * @param snapshots
     */
    public static void snapshotsAsCsv(java.lang.Iterable<Snapshot> snapshots) {
        for (Snapshot snapshot : snapshots) {
            String snapshotInfo = 
                String.format("%d,%d,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%d", 
                            snapshot.snapshotId(),
                            snapshot.timestampMillis(),
                            snapshot.operation(),
                            snapshot.summary().get("added-data-files"),
                            snapshot.summary().get("added-records"),
                            snapshot.summary().get("added-files-size"),
                            snapshot.summary().get("changed-partition-count"),
                            snapshot.summary().get("total-records"),
                            snapshot.summary().get("total-data-files"),
                            snapshot.summary().get("total-delete-files"),
                            snapshot.summary().get("total-position-deletes"),
                            snapshot.summary().get("total-equality-deletes"),
                            snapshot.manifestListLocation(),
                            snapshot.schemaId()
                            );
            System.out.println(snapshotInfo);
        }
    }
    
    /**
     * Represent a schema with comma delimited string for each of its columns
     * @param schema
     */
    public static void schemaAsCsv(Schema schema) {
        List<Types.NestedField> columns = schema.columns();
        for (Types.NestedField col : columns) {
            Type colType = col.type();
            String type;
            int precision = 0;
            int scale = 0;
            switch (colType.typeId()) {
                case BOOLEAN:
                    type = "boolean";
                    break;
                case INTEGER:
                    type = "int";
                    break;
                case LONG:
                    type = "long";
                    break;
                case FLOAT:
                    type = "float";
                    break;
                case DOUBLE:
                    type = "double";
                    break;
                case STRING:
                    type = "string";
                    break;
                case UUID:
                    type = "uuid";
                    break;
                case FIXED:
                    type = "fixed";
                    Types.FixedType fixed = (Types.FixedType) colType;
                    precision = fixed.length();
                    break;
                case BINARY:
                    type = "binary";
                    break;
                case DECIMAL:
                    type = "decimal";
                    Types.DecimalType decimal = (Types.DecimalType) colType;
                    precision = decimal.precision();
                    scale = decimal.scale();
                    break;
                case DATE:
                    type = "date";
                    break;
                case TIME:
                    type = "time";
                    break;
                case TIMESTAMP:
                    if (colType == Types.TimestampType.withZone()) {
                        type = "timestamp";
                    } else {
                        type = "timestamptz";
                    }
                    break;
                default:
                    throw new IllegalArgumentException("Unsupported column type");
            }
            
            String columnInfo = String.format("%d,%s,%s,%d,%d,%b", col.fieldId(), col.name(), type, precision, scale, col.isRequired());
            System.out.println(columnInfo);
        }
    }
    
}
