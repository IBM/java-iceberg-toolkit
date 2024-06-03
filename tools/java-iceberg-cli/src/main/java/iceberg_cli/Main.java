/**
 * (c) Copyright IBM Corp. 2023. All Rights Reserved.
 */

package iceberg_cli;

import iceberg_cli.utils.CliLogger;
import iceberg_cli.utils.SocketServer;
import iceberg_cli.security.PlainAuthenticator;

import java.util.Arrays;

import org.apache.log4j.Logger;

public class Main {
    private static Logger log;
    
    public static void main( String[] args ) {
        try {
            // Setup logging library
            CliLogger.setupLogging();

            log = CliLogger.getLogger();
            
            // Validate arguments
            if (args.length <= 0)
                throw new ArrayIndexOutOfBoundsException("No arguments provided");
            
            log.info("Request: " + Arrays.toString(args));

            // Start the server or run command
            if (args[0].equalsIgnoreCase("server")) {
                SocketServer server = new SocketServer();
                server.runServer();
            } else {
                String output = new IcebergApplication().processRequest(args);
                if (output != null)
                    System.out.println(output);
            }
        } catch (Exception e) {
            System.err.println("Error processing the request: " + e.getMessage());
            if (log != null)
                log.error("Error processing the request: " + e.getMessage());
            
            Throwable cause = e.getCause();
            if (cause != null) {
                System.err.println("Caused by: " + cause.getMessage());
                if (log != null)
                    log.error("Caused by: " + cause.getMessage());
            }
        } finally {
            PlainAuthenticator.cleanup();
        }
    }
}
