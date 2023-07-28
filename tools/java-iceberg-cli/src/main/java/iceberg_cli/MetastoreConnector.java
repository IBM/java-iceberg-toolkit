/**
 * (c) Copyright IBM Corp. 2022. All Rights Reserved.
 */

package iceberg_cli;

import java.util.*;
import java.io.IOException;
import java.io.UnsupportedEncodingException;
import java.net.URISyntaxException;
import org.apache.iceberg.exceptions.AlreadyExistsException;
import org.apache.iceberg.exceptions.NamespaceNotEmptyException;
import org.apache.iceberg.exceptions.NoSuchNamespaceException;
import org.apache.iceberg.exceptions.NoSuchTableException;
import org.apache.hadoop.hive.metastore.api.MetaException;
import org.apache.iceberg.FileScanTask;
import org.apache.iceberg.PartitionSpec;
import org.apache.iceberg.io.CloseableIterable;
import org.apache.iceberg.data.Record;
import org.apache.iceberg.Schema;
import org.apache.iceberg.Snapshot;
import org.apache.iceberg.catalog.Namespace;
import org.apache.iceberg.Table;
import org.apache.iceberg.TableMetadata;
import org.apache.iceberg.catalog.TableIdentifier;
import org.apache.iceberg.types.Types;
import org.apache.iceberg.types.Types.UUIDType;
import org.apache.thrift.TException;

import iceberg_cli.catalog.CustomCatalog;
import iceberg_cli.utils.Credentials;

import org.apache.iceberg.PartitionField;

public abstract class MetastoreConnector 
{
    protected Long m_snapshotId = null;

    protected String m_scanFilter = null;

    public MetastoreConnector(CustomCatalog catalog, String namespace, String tableName, Credentials creds) {
    }
    
    public abstract void setTableIdentifier(String namespace, String tableName);
            
    public abstract void loadTable() throws Exception;
    
    public abstract String getTableType() throws Exception;
    
    public abstract String getTableType(String database, String table) throws Exception;
    
    public abstract List<String> listTables(String namespace) throws Exception;
        
    public abstract boolean createTable(Schema schema, PartitionSpec spec, boolean overwrite) throws Exception;

    public abstract boolean alterTable(String newSchema) throws Exception;

    public abstract boolean dropTable() throws Exception;
    
    public abstract List<List<String>> readTable() throws Exception, UnsupportedEncodingException;

    public abstract Map<Integer, List<Map<String, String>>> getPlanFiles() throws IOException, URISyntaxException;
    
    public abstract Map<Integer, List<Map<String, String>>> getPlanTasks() throws IOException, URISyntaxException;

    public abstract String getTableLocation() throws Exception;

    public abstract String getTableDataLocation() throws Exception;
    
    public abstract Snapshot getCurrentSnapshot() throws Exception;
    
    public abstract Long getCurrentSnapshotId() throws Exception;

    public abstract Iterable<Snapshot> getListOfSnapshots() throws Exception;
    
    public abstract String writeTable(String record, String outputFile) throws Exception, UnsupportedEncodingException;
    
    public abstract boolean commitTable(String dataFileName) throws Exception;

    public abstract boolean rewriteFiles(String dataFileName) throws Exception;

    public abstract Schema getTableSchema();
    
    public abstract List<Namespace> listNamespaces() throws Exception;
    
    public abstract boolean createNamespace(Namespace namespace) throws Exception, AlreadyExistsException, UnsupportedOperationException;
    
    public abstract boolean dropNamespace(Namespace namespace) throws Exception, NamespaceNotEmptyException;
    
    public abstract java.util.Map<java.lang.String,java.lang.String> loadNamespaceMetadata(Namespace namespace) throws Exception, NoSuchNamespaceException;
    
    public abstract boolean renameTable(TableIdentifier from, TableIdentifier to) throws Exception, NoSuchTableException, AlreadyExistsException;
    
    public abstract PartitionSpec getSpec() throws Exception;
    
    public abstract String getUUID()  throws Exception;

    public void setSnapshotId(Long snapshotId) {
        this.m_snapshotId = snapshotId;
    }
    
    public void setScanFilter(String filter) {
        this.m_scanFilter = filter;
    }
    
    @SuppressWarnings("serial")
    class TableNotFoundException extends RuntimeException {
        public TableNotFoundException(String message) {
            super(message);
        }
    }
}
