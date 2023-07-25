package iceberg_cli;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInfo;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.DisplayName;
import org.apache.commons.cli.ParseException;
import org.apache.iceberg.Schema;
import org.apache.iceberg.SchemaParser;
import org.apache.iceberg.catalog.Namespace;
import org.json.JSONException;
import org.json.JSONObject;

import javax.servlet.ServletException;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.Comparator;
import java.util.List;
import java.util.UUID;

import iceberg_cli.catalog.ConfigLoader;
import iceberg_cli.catalog.CustomCatalog;
import iceberg_cli.utils.AwsCredentials;
import iceberg_cli.utils.Credentials;

class TestAlterTable {
    static String uri, warehouse, aws;
    static Credentials creds;

    @BeforeAll
    static void setup() throws JSONException, ParseException, IOException {
        uri = System.getenv("URI");
        if (uri == null) {
            System.out.println("URI environment variable not set, exiting...");
            System.exit(1);
        }
        warehouse = System.getenv("WAREHOUSE");
        if (warehouse == null) {
            System.out.println("WAREHOUSE environment variable not set, exiting...");
            System.exit(1);
        }
        // Create a dummy credentials object instead which will be populated using env variables
        creds = new AwsCredentials(new JSONObject());
    }

    CustomCatalog createCatalog(String namespace, String tablename) {
        CustomCatalog catalog = null;
        try {
            // Load catalog configuration
            catalog = new ConfigLoader().init("default", uri, warehouse);
        } catch (Exception e) {
            System.err.println("ERROR: " + e.getMessage());
        }

        // create namespace and the table. Insert a single record
        try {
            MetastoreConnector con = new IcebergConnector(catalog, null, null, creds);
            Namespace nmspc = Namespace.of(namespace);
            boolean status = con.createNamespace(nmspc);
            Assertions.assertEquals(true, status);
            con = new IcebergConnector(catalog, namespace, tablename, creds);
            Schema schema = SchemaParser.fromJson("{\"type\":\"struct\",\"schema-id\":0,\"fields\":[{\"id\":1,\"name\":\"c1\",\"required\":true,\"type\":\"int\"},{\"id\":2,\"name\":\"c2\",\"required\":false,\"type\":\"string\"},{\"id\":3,\"name\":\"c3\",\"required\":true,\"type\":\"double\"}]}");
            status = con.createTable(schema, null, false);
            Assertions.assertEquals(true, status);
            String record = "{\"records\":[{\"c1\":1,\"c2\":\"one\",\"c3\":1.1}]}";
            String dataFiles = con.writeTable(record, null);
            status = con.commitTable(dataFiles);
            Assertions.assertEquals(true, status);
        } catch (Exception e) {
            System.err.println("ERROR: " + e.getMessage());
        }
        return catalog;
    }

    void cleanup(CustomCatalog catalog, String namespace, String tablename) {
        try {
            MetastoreConnector con = new IcebergConnector(catalog, namespace, tablename, creds);
            boolean status = con.dropTable();
            Assertions.assertEquals(true, status);
            con = new IcebergConnector(catalog, null, null, creds);
            Namespace nmspc = Namespace.of(namespace);
            status = con.dropNamespace(nmspc);
            Assertions.assertEquals(true, status);
        } catch (Exception e) {
            System.err.println("ERROR: " + e.getMessage());
        }
    }

    @Test
    @DisplayName("Test_Alter_Table_Add_Column")
    void addColumn(TestInfo testInfo) throws ServletException {
        String testName = testInfo.getDisplayName().toLowerCase();
        String namespace = testName + "_" + UUID.randomUUID().toString().replace("-", "_");
        String tablename = testName;
        MetastoreConnector con;
        CustomCatalog catalog = createCatalog(namespace, tablename);
        try {
            con = new IcebergConnector(catalog, namespace, tablename, creds);
            String schema = "{\"add\":[{\"name\":\"c4\",\"type\":\"boolean\"}]}";
            boolean status = con.alterTable(schema);
            Assertions.assertEquals(true, status);
            String record = "{\"records\":[{\"c1\":2,\"c2\":\"two\",\"c3\":2.2,\"c4\":true}]}";
            String dataFiles = con.writeTable(record, null);
            status = con.commitTable(dataFiles);
            Assertions.assertEquals(true, status);
            List<List<String>> expected = new ArrayList<List<String>>() {{
                add(new ArrayList<>(Arrays.asList("1", "one", "1.1", "null")));
                add(new ArrayList<>(Arrays.asList("2", "two", "2.2", "true")));
            }};
            con.loadTable(); // reload table after write
            List<List<String>> actual = con.readTable();
            Collections.sort(actual, Comparator.comparing(list -> list.get(0)));
            Assertions.assertEquals(expected, actual);
        } catch (Throwable t) {
            throw new ServletException("Error: " + t.getMessage(), t);
        } finally {
            cleanup(catalog, namespace, tablename);
        }
    }

    @Test
    @DisplayName("Test_Alter_Table_Drop_Column")
    void dropColumn(TestInfo testInfo) throws ServletException {
        String testName = testInfo.getDisplayName().toLowerCase();
        String namespace = testName + "_" + UUID.randomUUID().toString().replace("-", "_");
        String tablename = testName;
        MetastoreConnector con;
        CustomCatalog catalog = createCatalog(namespace, tablename);
        try {
            con = new IcebergConnector(catalog, namespace, tablename, creds);
            String schema = "{\"drop\":[\"c1\"]}";
            boolean status = con.alterTable(schema);
            Assertions.assertEquals(true, status);
            String record = "{\"records\":[{\"c2\":\"two\",\"c3\":2.2}]}";
            String dataFiles = con.writeTable(record, null);
            status = con.commitTable(dataFiles);
            Assertions.assertEquals(true, status);
            List<List<String>> expected = new ArrayList<List<String>>() {{
                add(new ArrayList<>(Arrays.asList("one", "1.1")));
                add(new ArrayList<>(Arrays.asList("two", "2.2")));
            }};
            con.loadTable(); // reload table after write
            List<List<String>> actual = con.readTable();
            Collections.sort(actual, Comparator.comparing(list -> list.get(0)));
            Assertions.assertEquals(expected, actual);
        } catch (Throwable t) {
            throw new ServletException("Error: " + t.getMessage(), t);
        } finally {
            cleanup(catalog, namespace, tablename);
        }
    }

    @Test
    @DisplayName("Test_Alter_Table_Rename_Column")
    void renameColumn(TestInfo testInfo) throws ServletException {
        String testName = testInfo.getDisplayName().toLowerCase();
        String namespace = testName + "_" + UUID.randomUUID().toString().replace("-", "_");
        String tablename = testName;
        MetastoreConnector con;
        CustomCatalog catalog = createCatalog(namespace, tablename);
        try {
            con = new IcebergConnector(catalog, namespace, tablename, creds);
            String schema = "{\"rename\":[{\"name\":\"c2\",\"newName\":\"col2\"}]}";
            boolean status = con.alterTable(schema);
            Assertions.assertEquals(true, status);
            con.loadTable(); // reload table after alter
            String newSchema = con.getTableSchema().toString().replaceAll("\\s+", " ");
            Assertions.assertEquals("table { 1: c1: required int 2: col2: optional string 3: c3: required double }", newSchema);
        } catch (Throwable t) {
            throw new ServletException("Error: " + t.getMessage(), t);
        } finally {
            cleanup(catalog, namespace, tablename);
        }
    }

    @Test
    @DisplayName("Test_Alter_Table_Multi_Action_No_Drop")
    void multiActionNoDrop(TestInfo testInfo) throws ServletException {
        String testName = testInfo.getDisplayName().toLowerCase();
        String namespace = testName + "_" + UUID.randomUUID().toString().replace("-", "_");
        String tablename = testName;
        MetastoreConnector con;
        CustomCatalog catalog = createCatalog(namespace, tablename);
        try {
            con = new IcebergConnector(catalog, namespace, tablename, creds);
            String schema = "{\"add\":[{\"name\":\"c4\",\"type\":\"boolean\"}],\"rename\":[{\"name\":\"c2\",\"newName\":\"col2\"}]}";
            boolean status = con.alterTable(schema);
            Assertions.assertEquals(true, status);
            String record = "{\"records\":[{\"c1\":2,\"col2\":\"two\",\"c3\":2.2,\"c4\":true}]}";
            String dataFiles = con.writeTable(record, null);
            status = con.commitTable(dataFiles);
            Assertions.assertEquals(true, status);
            List<List<String>> expected = new ArrayList<List<String>>() {{
                add(new ArrayList<>(Arrays.asList("1", "one", "1.1", "null")));
                add(new ArrayList<>(Arrays.asList("2", "two", "2.2", "true")));
            }};
            con.loadTable(); // reload table after write
            List<List<String>> actual = con.readTable();
            Collections.sort(actual, Comparator.comparing(list -> list.get(0)));
            Assertions.assertEquals(expected, actual);
        } catch (Throwable t) {
            throw new ServletException("Error: " + t.getMessage(), t);
        } finally {
            cleanup(catalog, namespace, tablename);
        }
    }

    @Test
    @DisplayName("Test_Alter_Table_Multi_Action_With_Drop")
    void multiActionWithDrop(TestInfo testInfo) throws ServletException {
        String testName = testInfo.getDisplayName().toLowerCase();
        String namespace = testName + "_" + UUID.randomUUID().toString().replace("-", "_");
        String tablename = testName;
        MetastoreConnector con;
        CustomCatalog catalog = createCatalog(namespace, tablename);
        try {
            con = new IcebergConnector(catalog, namespace, tablename, creds);
            String schema = "{\"add\":[{\"name\":\"c4\",\"type\":\"boolean\"}],\"rename\":[{\"name\":\"c2\",\"newName\":\"col2\"}],\"drop\":[\"c1\"]}";
            boolean status = con.alterTable(schema);
            Assertions.assertEquals(false, status);
        } catch (Throwable t) {
            throw new ServletException("Error: " + t.getMessage(), t);
        } finally {
            cleanup(catalog, namespace, tablename);
        }
    }
}
