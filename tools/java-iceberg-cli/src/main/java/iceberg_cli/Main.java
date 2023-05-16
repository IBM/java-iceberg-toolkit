/**
 * (c) Copyright IBM Corp. 2023. All Rights Reserved.
 */

package iceberg;

public class Main {
    public static void main( String[] args ) {
        try {
            // Validate arguments
            if (args.length <= 0)
                throw new ArrayIndexOutOfBoundsException("No arguments provided");
            
            String output = new IcebergApplication().processRequest(args);
            System.out.println(output);
        } catch (Exception e) {
            System.err.println(e.getMessage());
        }
    }
}
