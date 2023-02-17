package iceberg;

import java.io.UnsupportedEncodingException;
import org.apache.iceberg.exceptions.AlreadyExistsException;
import org.apache.iceberg.exceptions.NamespaceNotEmptyException;
import org.apache.iceberg.exceptions.NoSuchNamespaceException;
import org.apache.iceberg.exceptions.NoSuchTableException;
import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.iceberg.PartitionSpec;
import org.apache.iceberg.Schema;
import org.apache.iceberg.Snapshot;
import org.apache.iceberg.TableMetadata;
import org.apache.iceberg.catalog.TableIdentifier;
import org.apache.iceberg.catalog.Namespace;
import org.apache.iceberg.hive.HiveSchemaUtil;
import java.util.*;
import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hive.metastore.HiveMetaStoreClient;
import org.apache.hadoop.hive.metastore.api.FieldSchema;
import org.apache.hadoop.hive.metastore.api.MetaException;
import org.apache.hadoop.hive.metastore.api.Table;
import org.apache.thrift.TException;

public class HiveConnector extends MetastoreConnector
{

    HiveConf hiveConf;
    HiveMetaStoreClient hiveClient;
    String database;
    String table;
    Table hive_table;
    
    public HiveConnector(String metastoreUri, String warehouse, String namespace, String tableName) throws MetaException {
        super(metastoreUri, warehouse, namespace, tableName);
        hiveConf = new HiveConf();
        hiveConf.set("hive.metastore.local", "false");
        hiveConf.setVar(HiveConf.ConfVars.METASTOREURIS, metastoreUri);
        
        hiveClient = new HiveMetaStoreClient(hiveConf);
        
        database = namespace;
        table = tableName;
    }
    
    public void loadTable() throws Exception {
        hive_table = hiveClient.getTable(database, table);
    }
    
    public void setTableIdentifier(String namespace, String tableName) {
        database = namespace;
        table = tableName;
    }
    
    public String getTableType(String database, String table) throws Exception {
        setTableIdentifier(database, table);
        loadTable();
        return hive_table.getTableType();
    }

    @Override
    public boolean createTable(Schema schema, PartitionSpec spec, boolean overwrite) throws Exception {
        // TODO Auto-generated method stub
        throw new Exception("Hive functionaility not supported yet.");
    }

    @Override
    public boolean dropTable() throws Exception {
        // TODO Auto-generated method stub
        throw new Exception("Hive functionaility not supported yet.");
    }

    @Override
    public List<List<String>> readTable() throws Exception, UnsupportedEncodingException {
        // TODO Auto-generated method stub
        throw new Exception("Hive functionaility not supported yet.");
    }

    private List<FileStatus> getFilesListRecursively(String location) throws IOException, URISyntaxException {
        List<FileStatus> files = new ArrayList<FileStatus>();
        FileSystem fs = FileSystem.get(new URI(location), hiveConf);
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
              hiveTable = hiveClient.getTable(database, table);
         } catch (TException e) {
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
    public List<String> listTables(String namespace) throws MetaException {
        // TODO Auto-generated method stub
        return hiveClient.getAllTables(namespace);
    }
    
    
    @Override
    public List<Namespace> listNamespaces() throws Exception {
        // TODO Auto-generated method stub
        throw new Exception("Hive functionaility not supported yet.");
    }
    
    @Override
    public java.util.Map<java.lang.String,java.lang.String> loadNamespaceMetadata(Namespace namespace) throws Exception {
        // TODO Auto-generated method stub
        throw new Exception("Hive functionaility not supported yet.");
    }
    
    @Override
    public boolean createNamespace(Namespace namespace) throws Exception, AlreadyExistsException, UnsupportedOperationException {
        // TODO Auto-generated method stub
        throw new Exception("Hive functionaility not supported yet.");
    }
    
    @Override
    public boolean dropNamespace(Namespace namespace) throws Exception, NamespaceNotEmptyException {
        // TODO Auto-generated method stub
        throw new Exception("Hive functionaility not supported yet.");
    }
    
    @Override
    public boolean renameTable(TableIdentifier from, TableIdentifier to) throws Exception, NoSuchTableException, AlreadyExistsException {
        // TODO Auto-generated method stub
        throw new Exception("Hive functionaility not supported yet.");
    }
    
    @Override
    public String getUUID() throws Exception {
        // TODO Auto-generated method stub
        throw new Exception("Hive functionaility not supported yet.");
    }

    @Override
    public String getTableLocation() throws Exception {
        // TODO Auto-generated method stub
        throw new Exception("Hive functionaility not supported yet.");
    }

    @Override
    public String getTableDataLocation() throws Exception {
        // TODO Auto-generated method stub
        throw new Exception("Hive functionaility not supported yet.");
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
        throw new Exception("Hive functionaility not supported yet.");
    }

    @Override
    public Iterable<Snapshot> getListOfSnapshots() throws Exception {
        return new ArrayList<Snapshot>();
    }

    @Override
    public String writeTable(String record, String outputFile) throws Exception, UnsupportedEncodingException {
        // TODO Auto-generated method stub
        // Where outputFile can be null
        throw new Exception("Hive functionaility not supported yet.");
    }

    @Override
    public boolean commitTable(String dataFileName) throws Exception {
        // TODO Auto-generated method stub
        throw new Exception("Hive functionaility not supported yet.");
    }

    @Override
    public Schema getTableSchema() {
        List<FieldSchema> schema;
        try {
             schema = hiveClient.getSchema(database, table);
        } catch (TException e) {
            System.err.println(e.getMessage());
            return null;
        }
        return HiveSchemaUtil.convert(schema);
    }
    
}