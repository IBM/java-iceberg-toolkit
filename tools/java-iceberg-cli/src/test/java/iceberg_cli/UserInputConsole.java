package iceberg_cli;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeAll;
import org.apache.iceberg.catalog.Namespace;
import org.apache.iceberg.exceptions.NoSuchNamespaceException;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.TestMethodOrder;

import iceberg_cli.*;

import org.junit.jupiter.api.MethodOrderer;
import org.junit.jupiter.api.Order;
import org.junit.jupiter.api.DisplayName;

import java.io.ByteArrayOutputStream;
import java.io.PrintStream;
import java.io.FileOutputStream;
import java.io.FileDescriptor;
import java.util.ArrayList;
import java.util.Scanner;
import java.util.UUID;

import javax.servlet.ServletException;

@TestMethodOrder(MethodOrderer.OrderAnnotation.class)
class UserInputConsole {
    
    static String uri;
    static String warehouse;
    static String namespace;
    static String tablename;
    static Integer total_tests = 6;
    static Integer passed_tests = 0;
    static ArrayList <String> failed_tests = new ArrayList<String>();
    
    @BeforeAll
    static void setup() {
        uri = System.getenv("URI");
        if (uri == null) {
            System.out.println("URI environment variable not set");
            System.exit(1);
            
        }
        warehouse = System.getenv("WAREHOUSE");
        namespace = "test_setup" + UUID.randomUUID().toString().replace("-", "");
        tablename = "test_table";
        System.out.println("\n*** Starting Tests ***\n");
        System.out.println(String.valueOf(total_tests) + " Total Tests\n");
    }
    
    @Test
    @Order(1)
    @DisplayName("Test the functionality of createnamespace action")
    void createnamespace() throws ServletException {
        String[] args = new String[6];
        args[0] = "-u";
        args[1] = uri;
        args[2] = "-w";
        args[3] = warehouse;
        args[4] = "create";
        args[5] = namespace;
        
        try {
            System.out.println("Running test 1...");
            String out = new IcebergApplication().processRequest(args);
            Assertions.assertEquals("Operation successful? true", out);
            System.out.println("Test 1 completed");
            passed_tests += 1;
        } catch (Throwable t) {
            failed_tests.add("createnamespace");
            throw new ServletException("Error: " + t.getMessage(), t);
        }
    }
    
    @Test
    @Order(2)
    @DisplayName("Test the functionality of create action")
    void createtable() throws ServletException {
        String[] args = new String[5];
        args[0] = "-u";
        args[1] = uri;
        args[2] = "create";
        args[3] = namespace + "." + tablename;
        args[4] = "{\"type\":\"struct\",\"schema-id\":0,\"fields\":[{\"id\":1,\"name\":\"ID\",\"required\":true,\"type\":\"int\"},{\"id\":2,\"name\":\"Name\",\"required\":true,\"type\":\"string\"},{\"id\":3,\"name\":\"Price\",\"required\":true,\"type\":\"double\"},{\"id\":4,\"name\":\"Purchase_date\",\"required\":true,\"type\":\"timestamp\"}]}";
        
        try {
            System.out.println("Running test 2...");
            String out = new IcebergApplication().processRequest(args);
            Assertions.assertEquals("Operation successful? true", out);
            System.out.println("Test 2 completed");
            passed_tests += 1;
        } catch (Throwable t) {
            failed_tests.add("createtable");
            throw new ServletException("Error: " + t.getMessage(), t);
        }
    }
    
    @Test
    @Order(3)
    @DisplayName("Test the functionality of write action")
    void writetable() throws ServletException {
        String[] args = new String[5];
        args[0] = "-u";
        args[1] = uri;
        args[2] = "write";
        args[3] = namespace + "." + tablename;
        args[4] = "{\"records\":[{\"ID\":1,\"Name\":\"Testing\",\"Price\": 1000,\"Purchase_date\":\"2022-11-09T12:13:54.480\"}]}";
        
        try {
            System.out.println("Running test 3...");
            String out = new IcebergApplication().processRequest(args);
            Assertions.assertEquals("Operation successful? true", out);    
            System.out.println("Test 3 completed");
            passed_tests += 1;
        } catch (Throwable t) {
            failed_tests.add("writetable");
            throw new ServletException("Error: " + t.getMessage(), t);
        }
    }
    
    
    @Test
    @Order(4)
    @DisplayName("Test the functionality of listtables action")
    void listtables() throws ServletException {
        String[] args = new String[4];
        args[0] = "-u";
        args[1] = uri;
        args[2] = "list";
        args[3] = namespace + "." + tablename;
        
        try {
            System.out.println("Running test 4...");
            String out = new IcebergApplication().processRequest(args);
            Assertions.assertEquals("LIST OF TABLES\nTABLE NAME\n" + tablename + "\n", out);
            System.out.println("Test 4 completed");
            passed_tests += 1;
        } catch (Throwable t) {
            failed_tests.add("listtables");
            throw new ServletException("Error: " + t.getMessage(), t);
        }
        
    }
    
    @Test
    @Order(5)
    @DisplayName("Test the functionality of drop action")
    void droptable() throws ServletException {
        String[] args = new String[4];
        args[0] = "-u";
        args[1] = uri;
        args[2] = "drop";
        args[3] = namespace + "." + tablename;
        
        try {
            System.out.println("Running test 5...");
            String out = new IcebergApplication().processRequest(args);
            Assertions.assertEquals("Operation successful? true", out);
            System.out.println("Test 5 completed");
            passed_tests += 1;
        } catch (Throwable t) {
            failed_tests.add("droptable");
            throw new ServletException("Error: " + t.getMessage(), t);
        }
        
    }
    
    @Test
    @Order(6)
    @DisplayName("Test the functionality of dropnamespace action")
    void dropnamespace() throws ServletException {
        String[] args = new String[4];
        args[0] = "-u";
        args[1] = uri;
        args[2] = "drop";
        args[3] = namespace;
                  
        try {
            System.out.println("Running test 6...");
            String out = new IcebergApplication().processRequest(args);
            Assertions.assertEquals("Operation successful? true", out);
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
