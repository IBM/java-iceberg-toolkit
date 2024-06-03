package iceberg_cli.utils;

import iceberg_cli.HiveConnector;
import iceberg_cli.IcebergConnector;
import iceberg_cli.MetastoreConnector;
import iceberg_cli.catalog.*;

public class CatalogUtils {
    public enum TableFormat {
        ICEBERG,
        HIVE
    }
    
    private static MetastoreConnector autoDetectConnector(CustomCatalog catalog, String namespace,
            String table, Credentials creds) throws Exception {
        HiveConnector hiveConn = new HiveConnector(catalog, namespace, table, creds);
        // For actions on namespaces, a table name may or may not be provided
        // Use IcebergConnector as default
        if (table != null) {
            String tableType = hiveConn.getTableType(namespace, table).toUpperCase();
            try {
                if (TableFormat.valueOf(tableType) == TableFormat.ICEBERG) {
                    hiveConn.close();
                    return new IcebergConnector(catalog, namespace, table, creds);
                }
            } catch (IllegalArgumentException e) {
                return hiveConn;
            }
        }
        hiveConn.close();
        return new IcebergConnector(catalog, namespace, table, creds);
    }
    
    public static MetastoreConnector getConnector(String catalogName, String format, String uri, String warehouse,
            String namespace, String table, Credentials creds) throws Exception {
        MetastoreConnector conn = null;
        // Load catalog information
        CustomCatalog catalog = new ConfigLoader().init(catalogName, uri, warehouse);
        
        // Auto detect format
        if (format == null) {
            return autoDetectConnector(catalog, namespace, table, creds);
        }
        
        // Make sure format is uppercase
        format = format.toUpperCase();
        // Check table format
        if (TableFormat.valueOf(format) == TableFormat.HIVE)
            conn = new HiveConnector(catalog, namespace, table, creds);
        else if (TableFormat.valueOf(format) == TableFormat.ICEBERG)
            conn = new IcebergConnector(catalog, namespace, table, creds);
        else
            conn = autoDetectConnector(catalog, namespace, table, creds);
        
        return conn;
    }
}
