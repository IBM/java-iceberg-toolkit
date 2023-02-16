/**
 * (c) Copyright IBM Corp. 2022. All Rights Reserved.
 */

package iceberg;

import iceberg.utils.PrintUtils;
import iceberg.cli.Parser;

import java.io.IOException;
import java.io.UnsupportedEncodingException;
import java.net.URISyntaxException;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.commons.cli.*;
import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.metastore.api.MetaException;
import org.apache.iceberg.PartitionSpec;
import org.apache.iceberg.Schema;
import org.apache.iceberg.SchemaParser;
import org.apache.iceberg.Snapshot;
import org.apache.iceberg.catalog.Namespace;
import org.apache.iceberg.catalog.TableIdentifier;
import org.apache.thrift.TException;

public class IcebergApplication {
    /**
     * @param args
     */
    public String processRequest( String[] args ) throws Exception
    {
        String output = null;
        
        Parser parser = new Parser();
        parser.parseArguments(args);
        
        // Get parameters
        String uri = parser.uri();
        String warehouse = parser.warehouse();
        String namespace = parser.namespace();
        String action = parser.command();
        String tableName = parser.table();
        String tableFormat = parser.tableFormat();
        String outputFormat = parser.outputFormat();
        String schemaJsonString = parser.getPositionalArg("schema");
        
        // Initialize HiveCatalog
        MetastoreConnector connector;
        if (tableFormat.equals("hive"))
            connector = new HiveConnector(uri, warehouse, namespace, tableName);
        else if (tableFormat.equals("iceberg"))
            connector = new IcebergConnector(uri, warehouse, namespace, tableName);
        else
            throw new ParseException("Unsupported table format: " + tableFormat);
        
        PrintUtils printUtils = new PrintUtils(connector, outputFormat);
        // Perform action
        switch (action) {
        case "read":
            output = printUtils.printTable();
            break;
        case "create":
            if (tableName != null) {
                if (schemaJsonString == null)
                    throw new ParseException("Missing required argument: schema");
                Schema schema = SchemaParser.fromJson(schemaJsonString);
                boolean overwrite = parser.overwrite();
                // TODO: Get PartitionSpec from user 
                PartitionSpec spec = PartitionSpec.unpartitioned();
                output = "Operation successful? " + connector.createTable(schema, spec, overwrite);
            } else if (namespace != null) {
                //Namespace argument validated above, validate warehouse argument 
                if (warehouse == null)
                    throw new ParseException("Missing required argument: warehouse");
                output = "Operation successful? " + connector.createNamespace(Namespace.of(namespace));
            }
            break;
        case "describe":
            if (tableName != null)
                output = printUtils.printTableDetails();
            else if (namespace != null)
                output = printUtils.printNamespaceDetails(namespace);
            break;
        case "list":
            if (namespace != null)
                output = printUtils.printTables(namespace);
            else {
                boolean fetchAll = parser.fetchAll();
                if (fetchAll)
                    output = printUtils.printAllTables();
                else
                    output = printUtils.printNamespaces();
            }
            break;
        case "location":
            output = connector.getTableLocation();
            break;
        case "files":
            output = printUtils.printFiles();
            break;
        case "snapshot":
            boolean fetchAll = parser.fetchAll();
            if (fetchAll)
                output = printUtils.printSnapshots();
            else
                output = printUtils.printCurrentSnapshot();
            break;
        case "rename":
            String tableNewNameString = parser.getPositionalArg("to_identifier");
            TableIdentifier tableOrigName = TableIdentifier.of(namespace, tableName);
            TableIdentifier tableNewName = TableIdentifier.parse(tableNewNameString);
            output = "Operation successful? " + connector.renameTable(tableOrigName, tableNewName);
            break;
        case "schema":
            output = printUtils.printSchema();
            break;
        case "spec":
            output = printUtils.printSpec();
            break;
        case "type":
            output = connector.getTableType(namespace, tableName);
            break;
        case "uuid":
            output = printUtils.printUUID();
            break;
        case "write":
            String record = parser.getPositionalArg("records");
            String outputFile = parser.outputFile();
            String dataFiles = connector.writeTable(record, outputFile);
            output = "Operation successful? " + connector.commitTable(dataFiles);
            break;
        case "commit":
            String dataFile = parser.getPositionalArg("data-files");
            output = "Operation successful? " + connector.commitTable(dataFile);
            break;
        case "drop":
            if (tableName != null)
                output = "Operation successful? " + connector.dropTable();
            else if (namespace != null)
                output = "Operation successful? " + connector.dropNamespace(Namespace.of(namespace));
            break;
        default:
            System.err.println("Error: Invalid action");
            break;
        }
        
        return output;
    }
}
