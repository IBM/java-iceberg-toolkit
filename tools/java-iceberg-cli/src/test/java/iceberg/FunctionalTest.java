package iceberg;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.TestMethodOrder;
import org.junit.jupiter.api.MethodOrderer;
import org.junit.jupiter.api.Order;
import org.junit.jupiter.api.DisplayName;

import org.apache.iceberg.Schema;
import org.apache.iceberg.SchemaParser;
import org.apache.iceberg.catalog.Namespace;

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

import iceberg.*;

@TestMethodOrder(MethodOrderer.OrderAnnotation.class)
class FunctionalTest {
    
    static String uri;
    static String warehouse;
    static MetastoreConnector metaConn;
    static String namespace;
    static String tablename;
    static Integer total_tests = 6;
    static Integer passed_tests = 0;
    static ArrayList <String> failed_tests = new ArrayList<String>();
    
    @BeforeAll
    static void setup() {
        Scanner input = new Scanner(System.in);
        uri = System.getenv("URI");
        if (uri == null) {
            System.out.println("URI environment variable not set. Enter URI:");
            uri = input.next();
        }
        warehouse = System.getenv("WAREHOUSE");
        if (warehouse == null) {
            System.out.println("WAREHOUSE environment variable not set. Enter WAREHOUSE:");
            warehouse = input.next();
        }
        metaConn = new IcebergConnector(uri, warehouse, null, null);
        namespace = "test_setup" + UUID.randomUUID().toString().replace("-", "");
        tablename = "test_table";
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
            ByteArrayOutputStream byteStream = new ByteArrayOutputStream();
            System.setOut(new PrintStream(byteStream));
            
            metaConn.createNamespace(nmspc);
            
            String out = byteStream.toString();
            System.setOut(new PrintStream(new FileOutputStream(FileDescriptor.out)));
            Assertions.assertEquals(out, "Namespace " + namespace + " created\n");
            System.out.println("Test 1 completed");
            passed_tests += 1;
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
            metaConn = new IcebergConnector(uri, warehouse, namespace, tablename);
            Schema schema = SchemaParser.fromJson("{\"type\":\"struct\",\"schema-id\":0,\"fields\":[{\"id\":1,\"name\":\"ID\",\"required\":true,\"type\":\"int\"},{\"id\":2,\"name\":\"Name\",\"required\":true,\"type\":\"string\"},{\"id\":3,\"name\":\"Price\",\"required\":true,\"type\":\"double\"},{\"id\":4,\"name\":\"Purchase_date\",\"required\":true,\"type\":\"timestamp\"}]}");
            
            System.out.println("Running test 2...");
            ByteArrayOutputStream byteStream = new ByteArrayOutputStream();
            System.setOut(new PrintStream(byteStream));
            
            metaConn.createTable(schema, null, false);
            
            String out = byteStream.toString();
            System.setOut(new PrintStream(new FileOutputStream(FileDescriptor.out)));
            Assertions.assertEquals(out, "Creating the table " + namespace + "." + tablename + "\nTable created successfully\n");        
            System.out.println("Test 2 completed");
            passed_tests += 1;
        } catch (Throwable t) {
            failed_tests.add("createtable");
            throw new ServletException("Error: " + t.getMessage(), t);
        }
    }
    
    @Test
    @Order(3)
    @DisplayName("Test the functionality of write table")
    void writetable() throws ServletException {
        try {
            metaConn = new IcebergConnector(uri, warehouse, namespace, tablename);
            String record = "{\"records\":[{\"ID\":1,\"Name\":\"Testing\",\"Price\": 1000,\"Purchase_date\":\"2022-11-09T12:13:54.480\"}]}";
            
            System.out.println("Running test 3...");
            ByteArrayOutputStream byteStream = new ByteArrayOutputStream();
            System.setOut(new PrintStream(byteStream));
            
            String dataFiles = metaConn.writeTable(record, null);
            metaConn.commitTable(dataFiles);
            
            String out = byteStream.toString();
            System.setOut(new PrintStream(new FileOutputStream(FileDescriptor.out)));
            String out_split = out.split("\n", 0)[4];
            Assertions.assertEquals(out_split, "Txn Complete!");    
            System.out.println("Test 3 completed");
            passed_tests += 1;
        } catch (Throwable t) {
            failed_tests.add("writetable");
            throw new ServletException("Error: " + t.getMessage(), t);
        }
    }

    @Test
    @Order(4)
    @DisplayName("Test the functionality of read table")
    void readtable() throws ServletException {
        try {
            metaConn = new IcebergConnector(uri, warehouse, namespace, tablename);
            List<List<String>> expected = Arrays.asList(Arrays.asList("1", "Testing", "1000.0", "2022-11-09T12:13:54.480"));
            
            System.out.println("Running test 4...");
            System.setOut(new PrintStream(OutputStream.nullOutputStream()));
            
            List<List<String>> actual = metaConn.readTable();
            
            System.setOut(new PrintStream(new FileOutputStream(FileDescriptor.out)));    
            Assertions.assertEquals(expected, actual);    
            System.out.println("Test 4 completed");
            passed_tests += 1;
        } catch (Throwable t) {
            failed_tests.add("readtable");
            throw new ServletException("Error: " + t.getMessage(), t);
        }
    }
    
    @Test
    @Order(5)
    @DisplayName("Test the functionality of drop table")
    void droptable() throws ServletException {
        try {
            metaConn = new IcebergConnector(uri, warehouse, namespace, tablename);
            
            System.out.println("Running test 5...");
            ByteArrayOutputStream byteStream = new ByteArrayOutputStream();
            System.setOut(new PrintStream(byteStream));
            
            metaConn.dropTable();
            
            String out = byteStream.toString();
            System.setOut(new PrintStream(new FileOutputStream(FileDescriptor.out)));
            Assertions.assertEquals(out, "Dropping the table " + namespace + "." + tablename + "\nTable dropped successfully\n");    
            System.out.println("Test 5 completed");
            passed_tests += 1;
        } catch (Throwable t) {
            failed_tests.add("droptable");
            throw new ServletException("Error: " + t.getMessage(), t);
        }
    }    
    
    @Test
    @Order(6)
    @DisplayName("Test the functionality of drop namespace")
    void dropnamespace() throws ServletException {
        try {
            Namespace nmspc = Namespace.of(namespace);
            
            System.out.println("Running test 6...");
            ByteArrayOutputStream byteStream = new ByteArrayOutputStream();
            System.setOut(new PrintStream(byteStream));
            
            metaConn.dropNamespace(nmspc);
            
            String out = byteStream.toString();
            System.setOut(new PrintStream(new FileOutputStream(FileDescriptor.out)));
            Assertions.assertEquals(out, "Namespace " + namespace + " dropped\n");
            System.out.println("Test 6 completed");
            passed_tests += 1;
        } catch (Throwable t) {
            failed_tests.add("dropnamespace");
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
    }
    
}