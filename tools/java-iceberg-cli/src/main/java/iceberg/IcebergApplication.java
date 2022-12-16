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
    public static void main( String[] args )
    {
        try {
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
            
            // Perform action
            switch (action) {
            case "read":
                PrintUtils.printTable(connector, outputFormat);
                break;
            case "create":
                if (tableName != null) {
                    if (schemaJsonString == null)
                        throw new ParseException("Missing required argument: schema");
                    Schema schema = SchemaParser.fromJson(schemaJsonString);
                    boolean overwrite = parser.overwrite();
                    // TODO: Get PartitionSpec from user 
                    PartitionSpec spec = PartitionSpec.unpartitioned();
                    connector.createTable(schema, spec, overwrite);
                } else if (namespace != null) {
                    //Namespace argument validated above, validate warehouse argument 
                    if (warehouse == null)
                        throw new ParseException("Missing required argument: warehouse");
                    connector.createNamespace(Namespace.of(namespace));
                }
                break;
            case "describe":
                if (tableName != null)
                    PrintUtils.printTableDetails(connector, outputFormat);
                else if (namespace != null)
                    PrintUtils.printNamespaceDetails(connector, outputFormat, namespace);
                break;
            case "list":
                if (namespace != null)
                    PrintUtils.printIcebergTables(connector, outputFormat, namespace, uri, warehouse);
                else
                    PrintUtils.printNamespaces(connector, outputFormat);
                break;
            case "files":
                PrintUtils.printFiles(connector, outputFormat);
                break;
            case "snapshot":
                boolean fetchAll = parser.fetchAll();
                if (fetchAll)
                    PrintUtils.printSnapshots(connector, outputFormat);
                else
                    PrintUtils.printCurrentSnapshot(connector, outputFormat);
                break;
            case "rename":
                String tableNewNameString = parser.getPositionalArg("to_identifier");
                TableIdentifier tableOrigName = TableIdentifier.of(namespace, tableName);
                TableIdentifier tableNewName = TableIdentifier.parse(tableNewNameString);
                connector.renameTable(tableOrigName, tableNewName);
                break;
            case "schema":
                PrintUtils.printSchema(connector, outputFormat);
                break;
            case "spec":
                PrintUtils.printSpec(connector);
                break;
            case "uuid":
                PrintUtils.printUUID(connector);
                break;
            case "write":
                String record = parser.getPositionalArg("records");
                String outputFile = parser.outputFile();
                String dataFiles = connector.writeTable(record, outputFile);
                connector.commitTable(dataFiles);
                break;
            case "commit":
                String dataFile = parser.getPositionalArg("data-files");
                connector.commitTable(dataFile);
                break;
            case "drop":
                if (tableName != null)
                    connector.dropTable();
                else if (namespace != null)
                    connector.dropNamespace(Namespace.of(namespace));
                break;
            default:
                System.err.println("Error: Invalid action");
                break;
            }
        
        } catch (ParseException exp) {
            System.err.println("Error parsing arguments: " + exp.getMessage());
        } catch (MetaException exp) {
            System.err.println(exp.getMessage());
        } catch (Exception exp) {
            System.err.println(exp.getMessage());
        }
    }
}
