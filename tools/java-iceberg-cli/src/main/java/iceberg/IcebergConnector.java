/**
 * (c) Copyright IBM Corp. 2022. All Rights Reserved.
 */

package iceberg;

import iceberg.utils.DataConversion;

import java.util.*;
import java.io.IOException;
import java.io.UnsupportedEncodingException;
import org.apache.iceberg.exceptions.NoSuchNamespaceException;
import org.apache.iceberg.exceptions.NoSuchTableException;
import org.apache.iceberg.exceptions.NamespaceNotEmptyException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.shaded.com.google.common.collect.ImmutableList;
import org.apache.iceberg.AppendFiles;
import org.apache.iceberg.CombinedScanTask;
import org.apache.iceberg.DataFile;
import org.apache.iceberg.DataFiles;
import org.apache.iceberg.FileFormat;
import org.apache.iceberg.FileScanTask;
import org.apache.iceberg.HasTableOperations;
import org.apache.iceberg.PartitionSpec;
import org.apache.iceberg.io.CloseableIterable;
import org.apache.iceberg.io.FileAppender;
import org.apache.iceberg.io.LocationProvider;
import org.apache.iceberg.io.OutputFile;
import org.apache.iceberg.parquet.Parquet;
import org.apache.iceberg.hive.HiveCatalog;
import org.apache.iceberg.data.parquet.GenericParquetWriter;
import org.apache.iceberg.exceptions.AlreadyExistsException;
import org.apache.iceberg.data.Record;
import org.apache.iceberg.data.GenericRecord;
import org.apache.iceberg.data.IcebergGenerics;
import org.apache.iceberg.Schema;
import org.apache.iceberg.Snapshot;
import org.apache.iceberg.Table;
import org.apache.iceberg.TableScan;
import org.apache.iceberg.Transaction;
import org.apache.iceberg.aws.s3.S3FileIO;
import org.apache.iceberg.catalog.Namespace;
import org.apache.iceberg.catalog.TableIdentifier;
import org.apache.iceberg.types.Type;
import org.apache.iceberg.types.Types;
import org.apache.iceberg.util.SerializableSupplier;
import org.apache.thrift.TException;
import org.apache.iceberg.TableMetadata;
import org.json.JSONArray;
import org.json.JSONObject;

import com.amazonaws.regions.Regions;

import software.amazon.awssdk.auth.credentials.AwsBasicCredentials;
import software.amazon.awssdk.auth.credentials.StaticCredentialsProvider;
import software.amazon.awssdk.http.SdkHttpClient;
import software.amazon.awssdk.http.apache.ApacheHttpClient;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.s3.S3Client;

public class IcebergConnector extends MetastoreConnector
{
    HiveCatalog m_catalog;
    TableIdentifier m_tableIdentifier;
    Table iceberg_table;
    TableScan m_scan;

    public IcebergConnector(String metastoreUri, String warehouse, String namespace, String tableName) {
        // TODO: Get type of catalog that the user wants and then initialize accordingly
        super(metastoreUri, warehouse, namespace, tableName);
        initCatalog(metastoreUri, warehouse);
        if (tableName != null)
            initTableIdentifier(namespace, tableName);
    }
    
    private void initCatalog(String metastoreUri, String warehouse) {
        m_catalog = new HiveCatalog();
        
        // Set Hadoop configuration
        Configuration config = new Configuration();
        if (System.getenv("AWS_ACCESS_KEY_ID") != null)
            config.set("fs.s3a.access.key", System.getenv("AWS_ACCESS_KEY_ID"));
        if (System.getenv("AWS_SECRET_ACCESS_KEY") != null)
            config.set("fs.s3a.secret.key", System.getenv("AWS_SECRET_ACCESS_KEY"));
        if (warehouse != null)
            config.set("hive.metastore.warehouse.dir", warehouse);
        m_catalog.setConf(config);
        
        // Set properties
        Map <String, String> properties = new HashMap<String, String>();
        properties.put("uri", metastoreUri);
        if (warehouse != null)
            properties.put("warehouse", warehouse);
        
        // Initialize Hive catalog
        m_catalog.initialize("hive", properties);
    }
    
    private void initTableIdentifier(String namespace, String tableName) {
        m_tableIdentifier = TableIdentifier.of(namespace, tableName);
    }
    
    public void setTableIdentifier(String namespace, String tableName) {
        m_tableIdentifier = TableIdentifier.of(namespace, tableName);
    }

    public Table loadTable(TableIdentifier identifier) {
        // Check if the table exists
        if (!m_catalog.tableExists(identifier)) {
            throw new TableNotFoundException("ERROR: Table " + identifier + " does not exist");
        }

        Table table = m_catalog.loadTable(identifier);
        // Double check if the table was loaded properly
        if (table == null)
            throw new TableNotLoaded("ERROR Loading table: " + identifier);
        
        return table;
    }
    
    public void loadTable() {
        iceberg_table = loadTable(m_tableIdentifier);

        // Use snapshot passed by the user.
        // By default, use the latest snapshot.
        m_scan = iceberg_table.newScan();
        if (m_snapshotId != null) {
            m_scan = m_scan.useSnapshot(m_snapshotId);
        }
    }
    
    public boolean createTable(Schema schema, PartitionSpec spec, boolean overwrite) {
        if (m_catalog.tableExists(m_tableIdentifier)) {
            if (overwrite) {
                // To overwrite an existing table, drop it first
                m_catalog.dropTable(m_tableIdentifier);
            } else {
                throw new RuntimeException("Table " + m_tableIdentifier + " already exists");
            }
        }
        
        System.out.println("Creating the table " + m_tableIdentifier);
        m_catalog.createTable(m_tableIdentifier, schema, spec);
        System.out.println("Table created successfully");
        
        return true;
    }
    
    public boolean dropTable() {
        loadTable();
        
        System.out.println("Dropping the table " + m_tableIdentifier);
        if (m_catalog.dropTable(m_tableIdentifier)) {
            System.out.println("Table dropped successfully");
            return true;
        }
        return false;
    }
    
    public List<List<String>> readTable() throws UnsupportedEncodingException {
        loadTable();
        
        // Get records
        System.out.println("Records in " + m_tableIdentifier + " :");
        IcebergGenerics.ScanBuilder scanBuilder = IcebergGenerics.read(iceberg_table);
        // Use specified snapshot, latest by default
        CloseableIterable<Record> records = scanBuilder.useSnapshot(m_scan.snapshot().snapshotId()).build();
        List<List<String>> output = new ArrayList<List<String>>();
        for (Record record : records) {
            int numFields = record.size();
            List<String> rec = new ArrayList<String>(numFields);
            for(int x = 0; x < numFields; x++) {
                // A field can be optional, add a check for null values
                Object value = record.get(x);
                rec.add(value == null ? "null" : value.toString());
            }
            output.add(rec);
        }
        return output;
    }

    public Map<Integer, List<Map<String, String>>> getPlanFiles() {
        loadTable();
        
        Iterable<CombinedScanTask> scanTasks = m_scan.planTasks();
        Map<Integer, List<Map<String, String>>> tasks = new HashMap<Integer, List<Map<String, String>>>();
        int index = 0;
        for (CombinedScanTask scanTask : scanTasks) {
            List<Map<String, String>> taskMapList = new ArrayList<Map<String, String>>();
            for (FileScanTask fileTask : scanTask.files()) {
                Map<String, String> taskMap = new HashMap<String, String>();
                DataFile file = fileTask.file();
                taskMap.put("content", file.content().toString());
                taskMap.put("file_path", file.path().toString());
                taskMap.put("file_format", file.format().toString());
                taskMap.put("start", Long.toString(fileTask.start()));
                taskMap.put("length", Long.toString(fileTask.length()));
                taskMap.put("spec", fileTask.spec().toString());
                taskMap.put("residual", fileTask.residual().toString());
                taskMapList.add(taskMap);
            }
            tasks.put(index++, taskMapList);
        }
        
        return tasks;
    }
    
    public java.util.List<String> listTables(String namespace) {
        List<TableIdentifier> table_list = m_catalog.listTables(Namespace.of(namespace));
        List<String> table_list_return = new ArrayList<String>();
        for (TableIdentifier table : table_list) {
            table_list_return.add(table.toString());
        }
        return table_list_return;
    }
    
    public java.util.List<Namespace> listNamespaces() {
        return m_catalog.listNamespaces();
    }
    
    public boolean createNamespace(Namespace namespace) throws AlreadyExistsException, UnsupportedOperationException {
        m_catalog.createNamespace(namespace);
        System.out.println("Namespace " + namespace + " created");
        
        return true;
        
    }
    
    public boolean dropNamespace(Namespace namespace) throws NamespaceNotEmptyException {
        if(m_catalog.dropNamespace(namespace)) {
            System.out.println("Namespace " + namespace + " dropped");
            return true;
        }
        return false;
    }
    
    public boolean renameTable(TableIdentifier from, TableIdentifier to) throws NoSuchTableException, AlreadyExistsException {
        m_catalog.renameTable(from, to);
        System.out.println("Table " + from + " renamed to " + to);
        
        return true;
    }
    
    public java.util.Map<java.lang.String,java.lang.String> loadNamespaceMetadata(Namespace namespace) throws NoSuchNamespaceException {
        return m_catalog.loadNamespaceMetadata(namespace);
    }
    
    public String getTableLocation() {
        loadTable();
        
        String tableLocation = iceberg_table.location();
        
        // Remove trailing backslash
        if (tableLocation.endsWith("/"))
            return tableLocation.substring(0, tableLocation.length() - 1);
        return tableLocation;
    }

    public String getTableDataLocation() {
        loadTable();

        LocationProvider provider = iceberg_table.locationProvider();
        String dataLocation = provider.newDataLocation("");
        
        // Remove trailing backslash
        if (dataLocation.endsWith("/"))
            return dataLocation.substring(0, dataLocation.length() - 1);
        return dataLocation;
    }
    
    public PartitionSpec getSpec() {
        loadTable();

        PartitionSpec spec = iceberg_table.spec();
        
        return spec;
    }
    
    public String getUUID() {
        loadTable();
        TableMetadata metadata = ((HasTableOperations) iceberg_table).operations().current();
        return metadata.uuid();
    }
         
    public Snapshot getCurrentSnapshot() {
        loadTable();
        
        return m_scan.snapshot();
    }
    
    public Long getCurrentSnapshotId() {
        loadTable();
        
        Snapshot snapshot = getCurrentSnapshot();
        if (snapshot != null)
            return snapshot.snapshotId();
        return null;
    }

    public java.lang.Iterable<Snapshot> getListOfSnapshots() {
        loadTable();

        java.lang.Iterable<Snapshot> snapshots = iceberg_table.snapshots();
        
        return snapshots;
    }
    
    public String writeTable(String records, String outputFile) throws IOException {
        loadTable();
        
        System.out.println("Writing to the table " + m_tableIdentifier);
        
        // Check if outFilePath or name is passed by the user
        if (outputFile == null) {
            outputFile = String.format("%s/icebergdata-%s.parquet", getTableDataLocation(), UUID.randomUUID());
        }
        
        JSONObject result = new JSONObject();
        JSONArray files = new JSONArray();
        
        Schema schema = iceberg_table.schema();
        ImmutableList.Builder<Record> builder = ImmutableList.builder();
        
        JSONArray listOfRecords = new JSONObject(records).getJSONArray("records");
        for (int index = 0; index < listOfRecords.length(); ++index) {
            JSONObject fields = listOfRecords.getJSONObject(index);
            List<Types.NestedField> columns = schema.columns();
            String[] fieldNames = JSONObject.getNames(fields);
            // Verify if input columns are the same number as the required fields
            // Optional fields shouldn't be part of the check
            if (fieldNames.length > columns.size()) 
                throw new IllegalArgumentException("Record has invalid number of fields");
            
            Record genericRecord = GenericRecord.create(schema);
            for (Types.NestedField col : columns) {
                String colName = col.name();
                Type colType = col.type();
                // Validate that a required field is present in the record
                if (!fields.has(colName)) {
                    if (col.isRequired())
                        throw new IllegalArgumentException("Record is missing a required field: " + colName);
                    else
                        continue;
                }
                
                // Trim the input value
                String value = fields.get(colName).toString().trim();
                
                // Check for null values
                if (col.isRequired() && value.equalsIgnoreCase("null"))
                    throw new IllegalArgumentException("Required field cannot be null: " + colName);
                
                // Store the value as an iceberg data type
                genericRecord.setField(colName, DataConversion.stringToIcebergType(value, colType));
            }
            builder.add(genericRecord.copy());
        }
                                            
        AwsBasicCredentials awsCreds = AwsBasicCredentials.create(
                System.getenv("AWS_ACCESS_KEY_ID"),
                System.getenv("AWS_SECRET_ACCESS_KEY"));
    
        SdkHttpClient client = ApacheHttpClient.builder()
                .maxConnections(100)
                .build();
            
        SerializableSupplier<S3Client> supplier = () -> S3Client.builder()
                .region(Region.of(System.getenv("AWS_REGION")))
                .credentialsProvider(StaticCredentialsProvider.create(awsCreds))
                .httpClient(client)
                .build();

        S3FileIO io = new S3FileIO(supplier);
        OutputFile location = io.newOutputFile(outputFile);
        System.out.println("New file created at: " + location);
                    
        FileAppender<Record> appender;
        appender = Parquet.write(location)
                        .schema(schema)
                        .createWriterFunc(GenericParquetWriter::buildWriter)
                        .build();
        appender.addAll(builder.build());
        io.close();
        appender.close();
        
        // Add file info to the JSON object
        JSONObject file = new JSONObject();
        file.put("file_path", outputFile);
        file.put("file_format", FileFormat.fromFileName(outputFile));
        file.put("file_size_in_bytes", appender.length());
        file.put("record_count", listOfRecords.length());
        files.put(file);
        
        result.put("files", files);
                
        return result.toString();
    }
    
    public boolean commitTable(String dataFiles) {
        loadTable();
        
        System.out.println("Commiting to the table " + m_tableIdentifier);
        
        PartitionSpec ps = iceberg_table.spec();

        AwsBasicCredentials awsCreds = AwsBasicCredentials.create(
                System.getenv("AWS_ACCESS_KEY_ID"),
                System.getenv("AWS_SECRET_ACCESS_KEY"));

        SdkHttpClient client = ApacheHttpClient.builder()
                .maxConnections(100)
                .build();

        SerializableSupplier<S3Client> supplier = () -> S3Client.builder()
                .region(Region.of(System.getenv("AWS_REGION")))
                .credentialsProvider(StaticCredentialsProvider.create(awsCreds))
                .httpClient(client)
                .build();
        
        S3FileIO io = new S3FileIO(supplier);
        
        JSONArray files = new JSONObject(dataFiles).getJSONArray("files");
        Transaction transaction = iceberg_table.newTransaction();
        AppendFiles append = transaction.newAppend();
        // Commit data files
        System.out.println("Starting Txn");
        for (int index = 0; index < files.length(); ++index) {
            JSONObject file = files.getJSONObject(index);
            String filePath = file.getString("file_path");
            OutputFile outputFile = io.newOutputFile(filePath);
            
            // FIXME: Replace the hard-coded values with the commented
            // out code in the next iteration
//                DataFile data = DataFiles.builder(ps)
//                         .withPath(outputFile.location())
//                         .withFormat(FileFormat.valueOf(file.getString("file_format")))
//                         .withFileSizeInBytes(file.getInt("file_size_in_bytes"))
//                         .withRecordCount(file.getInt("record_count"))
//                         .build();
            DataFile data = DataFiles.builder(ps)
                     .withPath(outputFile.location())
                     .withFormat(FileFormat.PARQUET)
                     .withFileSizeInBytes(2000)
                     .withRecordCount(1)
                     .build();
            
            append.appendFile(data);
        }
        append.commit();
        transaction.commitTransaction();
        io.close();
        System.out.println("Txn Complete!");
        
        return true;
    }

    public Schema getTableSchema() {
        loadTable();
        return m_scan.schema();
    }
    
    public String getTableType(String database, String table) throws Exception {
        setTableIdentifier(database, table);
        loadTable();
        if (iceberg_table == null) {
            return null;
        }
        return "ICEBERG";
    }
    
    @SuppressWarnings("serial")
    public class TableNotFoundException extends RuntimeException {
        public TableNotFoundException(String message) {
            super(message);
        }
    }
    
    @SuppressWarnings("serial")
    public class TableNotLoaded extends RuntimeException {
        public TableNotLoaded(String message) {
            super(message);
        }
    }
    
}
