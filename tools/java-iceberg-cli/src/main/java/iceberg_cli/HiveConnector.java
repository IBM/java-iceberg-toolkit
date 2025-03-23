package iceberg_cli;

import java.io.UnsupportedEncodingException;
import org.apache.iceberg.exceptions.AlreadyExistsException;
import org.apache.iceberg.exceptions.NamespaceNotEmptyException;
import org.apache.iceberg.exceptions.NoSuchTableException;
import org.apache.iceberg.hive.HiveClientPool;
import org.apache.iceberg.PartitionSpec;
import org.apache.iceberg.Schema;
import org.apache.iceberg.Snapshot;
import org.apache.iceberg.catalog.TableIdentifier;
import org.apache.iceberg.catalog.Namespace;
import org.apache.iceberg.hive.HiveSchemaUtil;
import java.util.*;
import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.metastore.HiveMetaStoreClient;
import org.apache.hadoop.hive.metastore.api.Database;
import org.apache.hadoop.hive.metastore.api.FieldSchema;
import org.apache.hadoop.hive.metastore.api.MetaException;
import org.apache.hadoop.hive.metastore.api.Table;
import org.apache.hadoop.hive.metastore.conf.MetastoreConf;
import org.apache.hadoop.hive.metastore.conf.MetastoreConf.ConfVars;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.thrift.TException;

import iceberg_cli.catalog.CustomCatalog;
import iceberg_cli.utils.Credentials;

public class HiveConnector extends MetastoreConnector
{

    Configuration conf;
    HiveClientPool clients;
    String database;
    String table;
    Table hiveTable;
    
    private static final int DEFAULT_POOL_SIZE = 2;
    
    public HiveConnector(CustomCatalog catalog, String namespace, String tableName, Credentials creds) throws MetaException, IOException {
        super(catalog, namespace, tableName, creds);
        
        // Get catalog configuration
        conf = catalog.getConf();
        
        // Set credentials, if any
        if (creds.isValid()) {
            conf.set("fs.s3a.access.key", creds.getValue("AWS_ACCESS_KEY_ID"));
            conf.set("fs.s3a.secret.key", creds.getValue("AWS_SECRET_ACCESS_KEY"));
            
            String endpoint = creds.getValue("ENDPOINT");
            if(endpoint != null) {
            	conf.set("fs.s3a.endpoint", endpoint);
            	// Set path style access for non-aws endpoints
            	conf.set("fs.s3a.path.style.access", "true");
            }
        }
        
        clients = new HiveClientPool(DEFAULT_POOL_SIZE, conf);
        
        database = namespace;
        table = tableName;
    }
    
    @Override
    public void close() {
        clients.close();
    }
    
    public void loadTable() throws Exception {
        hiveTable = clients.run(client -> client.getTable(database, table));
    }
    
    public Table loadTable(String database, String table) throws Exception {
        return clients.run(client -> client.getTable(database, table));
    }
    
    public void setTableIdentifier(String namespace, String tableName) {
        database = namespace;
        table = tableName;
    }
    
    private String type(Table table) {
        String tableType = table.getParameters().get("table_type");
        if (tableType != null)
            return tableType;
        return table.getTableType();
    }
    
    public String getTableType() throws Exception {
        if (hiveTable == null)
            loadTable();
        return type(hiveTable);
    }
    
    public String getTableType(String database, String table) throws Exception {
        Table hiveTable = loadTable(database, table);
        return type(hiveTable);
    }

    @Override
    public boolean createTable(Schema schema, PartitionSpec spec, boolean overwrite) throws Exception {
        // TODO Auto-generated method stub
        throw new Exception("Hive functionality not supported yet.");
    }

    @Override
    public boolean alterTable(String newSchema) throws Exception {
        throw new Exception("Hive functionality not supported yet.");
    }

    @Override
    public boolean dropTable() throws Exception {
        // TODO Auto-generated method stub
        throw new Exception("Hive functionality not supported yet.");
    }

    @Override
    public List<List<String>> readTable() throws Exception, UnsupportedEncodingException {
        // TODO Auto-generated method stub
        throw new Exception("Hive functionality not supported yet.");
    }

    private List<FileStatus> getFilesListRecursively(String location) throws IOException, URISyntaxException {
        List<FileStatus> files = new ArrayList<FileStatus>();
        FileSystem fs = FileSystem.get(new URI(location), conf);
        FileStatus[] fileStatus = fs.listStatus(new Path(location));

        for(FileStatus status : fileStatus) {
            if(status.isDirectory())
                files.addAll(getFilesListRecursively(status.getPath().toString()));
            else
                files.add(status);
        }
        return files;
    }
    
    @Override
    public Map<Integer, List<Map<String, String>>> getPlanFiles() throws IOException, URISyntaxException {
        org.apache.hadoop.hive.metastore.api.Table hiveTable;
         try {
              hiveTable = clients.run(client -> client.getTable(database, table));
         } catch (TException | InterruptedException e) {
             System.err.println("Error loading Hive table: " + e.getMessage());
             return null;
         }
         List<FileStatus> hiveFiles = getFilesListRecursively(hiveTable.getSd().getLocation());
         
         //Hive does not have any sophisticated scan planning, just assign one file per task
         int idx = 0;
         Map<Integer, List<Map<String, String>>> taskMapListList = new HashMap<Integer, List<Map<String, String>>>();
         List<Map<String, String>> taskMapList = new ArrayList<Map<String, String>>();
         for (FileStatus file : hiveFiles) {
             Map<String, String> taskMap = new HashMap<String, String>();
             taskMap.put("content", "DATA");
             taskMap.put("file_path", file.getPath().toString());
             String format = hiveTable.getSd().getOutputFormat().contains("parquet") ? "PARQUET" : "UNKNOWN";
             taskMap.put("file_format", format);
             taskMap.put("start", "0");
             taskMap.put("length", Long.toString(file.getLen()));
             taskMap.put("spec", "[]"); //not sure this matters..do empty
             taskMap.put("residual", "true"); //not sure either
             taskMapList.add(taskMap);
         }
         
         // FIXME: All files will be part of task 0 for the time being for testing.
         // Either move this inside the loop or figure out a mechanism to divide
         // the files into equal sized tasks.
         taskMapListList.put(idx++, taskMapList);
         
         return taskMapListList;
    }
    
    @Override
    public Map<Integer, List<Map<String, String>>> getPlanTasks() throws IOException, URISyntaxException {
        return getPlanFiles();
    }

    @Override
    public List<String> listTables(String namespace) throws TException, InterruptedException {
        return clients.run(client -> client.getAllTables(namespace));
    }
    
    
    @Override
    public List<Namespace> listNamespaces() throws Exception {
        return clients.run(client -> client.getAllDatabases().stream().map(Namespace::of).toList());
    }
    
    @Override
    public java.util.Map<java.lang.String,java.lang.String> loadNamespaceMetadata(Namespace namespace) throws Exception {
        Map<String, String> metadata = new HashMap<String, String>();
        metadata.put("location", clients.run(client -> client.getDatabase(namespace.toString()).getLocationUri()));
        return metadata;
    }
    
    @Override
    public boolean createNamespace(Namespace namespace) throws Exception, AlreadyExistsException, UnsupportedOperationException {
        // Get warehouse path
        String warehouse = clients.run(client -> client.getConfigValue(HiveConf.ConfVars.METASTORE_WAREHOUSE.varname, null));
        Database database = new Database(namespace.toString(), null, warehouse, new HashMap<String, String>());
        clients.run(
                client -> {
                    client.createDatabase(database);
                    return null;
                });
        System.out.println("Namespace " + namespace + " created");
        
        return true;
    }
    
    @Override
    public boolean dropNamespace(Namespace namespace) throws Exception, NamespaceNotEmptyException {
        // TODO Auto-generated method stub
        throw new Exception("Hive functionality not supported yet.");
    }
    
    @Override
    public boolean renameTable(TableIdentifier from, TableIdentifier to) throws Exception, NoSuchTableException, AlreadyExistsException {
        // TODO Auto-generated method stub
        throw new Exception("Hive functionality not supported yet.");
    }
    
    @Override
    public String getUUID() throws Exception {
        // TODO Auto-generated method stub
        throw new Exception("Hive functionality not supported yet.");
    }

    @Override
    public String getTableLocation() throws Exception {
        if (hiveTable == null)
            loadTable();
        
        return hiveTable.getSd().getLocation();
    }

    @Override
    public String getTableDataLocation() throws Exception {
        return null;
    }

    @Override
    public Snapshot getCurrentSnapshot() throws Exception {
        return null;
    }
    
    @Override
    public Long getCurrentSnapshotId() throws Exception {
        return null;
    }
    
    @Override
    public PartitionSpec getSpec() throws Exception {
        // TODO Auto-generated method stub
        throw new Exception("Hive functionality not supported yet.");
    }

    @Override
    public Iterable<Snapshot> getListOfSnapshots() throws Exception {
        // TODO Auto-generated method stub
        throw new Exception("Hive functionality not supported yet.");
    }

    @Override
    public String writeTable(String record, String outputFile) throws Exception, UnsupportedEncodingException {
        // TODO Auto-generated method stub
        // Where outputFile can be null
        throw new Exception("Hive functionality not supported yet.");
    }

    @Override
    public boolean commitTable(String dataFileName) throws Exception {
        // TODO Auto-generated method stub
        throw new Exception("Hive functionality not supported yet.");
    }

    @Override
    public boolean rewriteFiles(String dataFileName) throws Exception {
        // TODO Auto-generated method stub
        throw new Exception("Hive functionality not supported yet.");
    }

    @Override
    public Schema getTableSchema() {
        List<FieldSchema> schema;
        try {
             schema = clients.run(client -> client.getSchema(database, table));
        } catch (TException | InterruptedException e) {
            return null;
        }
        return HiveSchemaUtil.convert(schema);
    }
}
