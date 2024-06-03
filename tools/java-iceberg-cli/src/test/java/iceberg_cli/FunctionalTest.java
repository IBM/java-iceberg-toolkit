package iceberg_cli;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.TestMethodOrder;

import iceberg_cli.*;
import iceberg_cli.catalog.ConfigLoader;
import iceberg_cli.catalog.CustomCatalog;
import iceberg_cli.utils.AwsCredentials;
import iceberg_cli.utils.Credentials;

import org.junit.jupiter.api.MethodOrderer;
import org.junit.jupiter.api.Order;
import org.junit.jupiter.api.DisplayName;

import org.apache.iceberg.Schema;
import org.apache.iceberg.SchemaParser;
import org.apache.iceberg.catalog.Namespace;
import org.apache.iceberg.exceptions.NoSuchNamespaceException;
import org.json.JSONObject;

import javax.servlet.ServletException;

import java.io.ByteArrayOutputStream;
import java.io.FileDescriptor;
import java.io.FileOutputStream;
import java.io.OutputStream;
import java.io.PrintStream;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Scanner;
import java.util.UUID;

@TestMethodOrder(MethodOrderer.OrderAnnotation.class)
class FunctionalTest {
    
    static CustomCatalog catalog;
    static Credentials creds;
    static MetastoreConnector metaConn;
    static String namespace;
    static String tableName;
    static Integer total_tests = 0;
    static Integer passed_tests = 0;
    static ArrayList <String> failed_tests = new ArrayList<String>();
    
    static void markPass() {
        passed_tests++;
        total_tests++;
    }

    static void markFail(String test) {
        failed_tests.add(test);
        total_tests++;
    }

    @BeforeAll
    static void setup() {
        String uri = System.getenv("URI");
        if (uri == null) {
            System.out.println("Metastore \"URI\" environment variable not set, exiting...");
            System.exit(1);
            
        }
        String warehouse = System.getenv("WAREHOUSE");
        namespace = "test_setup" + UUID.randomUUID().toString().replace("-", "");
        tableName = "test_table";
        try {
            // Do not ask for credentials from the user
            // Create a dummy credentials object instead which will be populated using env variables
            creds = new AwsCredentials(new JSONObject());
            // Load catalog configuration
            catalog = new ConfigLoader().init("default", uri, warehouse);
            // Initialize connector using a dummy credentials object
            metaConn = new IcebergConnector(catalog, namespace, tableName, creds);
            if (warehouse == null) {
                warehouse = metaConn.loadNamespaceMetadata(Namespace.of("default")).get("location");
                catalog.setProperty("warehouse", warehouse);
                metaConn = new IcebergConnector(catalog, namespace, tableName, creds);
            }
        } catch (NoSuchNamespaceException e) {
            System.err.println("WAREHOUSE environment variable not set and no default warehouse was found, exiting...");
            System.exit(1);
        } catch (Exception e) {
            System.err.println("ERROR: " + e.getMessage());
            System.exit(1);
        }
        System.out.println("\n*** Starting Tests ***\n");
        System.out.println(String.valueOf(total_tests) + " Total Tests\n");
    }
    
    @Test
    @Order(1)
    @DisplayName("Test the functionality of create namespace")
    void createnamespace() throws ServletException {
        try {
            Namespace nmspc = Namespace.of(namespace);
            System.out.println("Running test 1...");
            boolean status = metaConn.createNamespace(nmspc);
            Assertions.assertEquals(true, status);
            System.out.println("Test 1 completed");
            markPass();
        } catch (Throwable t) {
            failed_tests.add("createnamespace");
            throw new ServletException("Error: " + t.getMessage(), t);
        }
    }
    
    @Test
    @Order(2)
    @DisplayName("Test the functionality of create table")
    void createtable() throws ServletException {
        try {
            Schema schema = SchemaParser.fromJson("{\"type\":\"struct\",\"schema-id\":0,\"fields\":[{\"id\":1,\"name\":\"ID\",\"required\":true,\"type\":\"int\"},{\"id\":2,\"name\":\"Name\",\"required\":true,\"type\":\"string\"},{\"id\":3,\"name\":\"Price\",\"required\":true,\"type\":\"double\"},{\"id\":4,\"name\":\"Purchase_date\",\"required\":true,\"type\":\"timestamp\"}]}");
            
            System.out.println("Running test 2...");
            boolean status = metaConn.createTable(schema, null, false);
            Assertions.assertEquals(true, status);
            System.out.println("Test 2 completed");
            markPass();
        } catch (Throwable t) {
            markFail("createtable");
            throw new ServletException("Error: " + t.getMessage(), t);
        }
    }
    
    @Test
    @Order(3)
    @DisplayName("Test the functionality of read on an empty table")
    void readEmptytable() throws ServletException {
        try {
            List<List<String>> expected = new ArrayList<List<String>>();
            
            System.out.println("Running test 3...");
            List<List<String>> actual = metaConn.readTable();
            Assertions.assertEquals(expected, actual);    
            System.out.println("Test 3 completed");
            markPass();
        } catch (Throwable t) {
            markFail("reademptytable");
            throw new ServletException("Error: " + t.getMessage(), t);
        }
    }
    
    @Test
    @Order(4)
    @DisplayName("Test the functionality of write table")
    void writetable() throws ServletException {
        try {
            String record = "{\"records\":[{\"ID\":1,\"Name\":\"Testing\",\"Price\": 1000,\"Purchase_date\":\"2022-11-09T12:13:54.480\"}]}";
            
            System.out.println("Running test 4...");
            String dataFiles = metaConn.writeTable(record, null);
            boolean status = metaConn.commitTable(dataFiles);
            Assertions.assertEquals(true, status); 
            System.out.println("Test 4 completed");
            markPass();
        } catch (Throwable t) {
            markFail("writetable");
            throw new ServletException("Error: " + t.getMessage(), t);
        }
    }

    @Test
    @Order(5)
    @DisplayName("Test the functionality of read table")
    void readtable() throws ServletException {
        try {
            List<List<String>> expected = Arrays.asList(Arrays.asList("1", "Testing", "1000.0", "2022-11-09T12:13:54.480"));
            
            System.out.println("Running test 5...");
            List<List<String>> actual = metaConn.readTable();
            Assertions.assertEquals(expected, actual);    
            System.out.println("Test 5 completed");
            markPass();
        } catch (Throwable t) {
            markFail("readtable");
            throw new ServletException("Error: " + t.getMessage(), t);
        }
    }

    @Test
    @Order(6)
    @DisplayName("Test the functionality of rewrite files")
    void rewritefiles() throws ServletException {
        try {
            metaConn = new IcebergConnector(catalog, namespace, tableName, creds);
            MetastoreConnector metaConnDup = new IcebergConnector(catalog, namespace, (tableName + "Dup"), creds);
            Schema schema = SchemaParser.fromJson("{\"type\":\"struct\",\"schema-id\":0,\"fields\":[{\"id\":1,\"name\":\"ID\",\"required\":true,\"type\":\"int\"},{\"id\":2,\"name\":\"Name\",\"required\":true,\"type\":\"string\"},{\"id\":3,\"name\":\"Price\",\"required\":true,\"type\":\"double\"},{\"id\":4,\"name\":\"Purchase_date\",\"required\":true,\"type\":\"timestamp\"}]}");
        
            System.out.println("Running test 8...");
            String record = "{\"records\":[{\"ID\":1,\"Name\":\"Testing\",\"Price\": 1000,\"Purchase_date\":\"2022-11-09T12:13:54.480\"}]}";
            String dataFiles = metaConn.writeTable(record, null);
            boolean status = metaConn.commitTable(dataFiles);
            Assertions.assertEquals(true, status); 
            status = metaConnDup.createTable(schema, null, false);
            String dataFilesDup = metaConnDup.writeTable(record, null);
            JSONObject result = new JSONObject();
            result.put("files_to_del", new JSONObject(dataFiles).getJSONArray("files"));
            result.put("files_to_add", new JSONObject(dataFilesDup).getJSONArray("files"));
            status = metaConn.rewriteFiles(result.toString());
            Assertions.assertEquals(true, status);
            // Clean up rewritten table
            status = metaConnDup.dropTable();
            Assertions.assertEquals(true, status);
            System.out.println("Test 8 completed");
            markPass();
        } catch (Throwable t) {
            markFail("rewritefiles");
            throw new ServletException("Error: " + t.getMessage(), t);
        }
    }
    
    @Test
    @Order(7)
    @DisplayName("Test the functionality of drop table")
    void droptable() throws ServletException {
        try {
            System.out.println("Running test 6...");
            boolean status = metaConn.dropTable();
            Assertions.assertEquals(true, status);
            System.out.println("Test 6 completed");
            markPass();
        } catch (Throwable t) {
            markFail("droptable");
            throw new ServletException("Error: " + t.getMessage(), t);
        }
    }    
    
    @Test
    @Order(8)
    @DisplayName("Test the functionality of drop namespace")
    void dropnamespace() throws ServletException {
        try {
            Namespace nmspc = Namespace.of(namespace);
            
            System.out.println("Running test 7...");
            boolean status = metaConn.dropNamespace(nmspc);
            Assertions.assertEquals(true, status);
            System.out.println("Test 7 completed");
            markPass();
        } catch (Throwable t) {
            markFail("dropnamespace");
            throw new ServletException("Error: " + t.getMessage(), t);
        }
    }
    
    @AfterAll
    static void result() {
        System.out.println("\n*** Results ***\n");
        System.out.println("Total Tests: " + String.valueOf(total_tests) + "    Tests Passed: " + String.valueOf(passed_tests) + "    Tests Failed: " + String.valueOf(total_tests - passed_tests));
        if(total_tests - passed_tests > 0) {
            System.out.println("List of Failed Test Cases: " + failed_tests);
            
        }
        
        metaConn.close();
    }
    
}