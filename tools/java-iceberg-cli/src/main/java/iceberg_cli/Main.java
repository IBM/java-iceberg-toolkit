/**
 * (c) Copyright IBM Corp. 2023. All Rights Reserved.
 */

package iceberg_cli;

import iceberg_cli.utils.CliLogger;
import iceberg_cli.utils.SocketServer;

public class Main {
    public static void main( String[] args ) {
        try {
            // Validate arguments
            if (args.length <= 0)
                throw new ArrayIndexOutOfBoundsException("No arguments provided");

            // Setup logging library
            CliLogger.setupLogging();

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
        }
    }
}
