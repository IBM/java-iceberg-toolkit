package iceberg_cli.security;

import java.io.IOException;

import iceberg_cli.catalog.CustomCatalog;

public class AuthProvider {
    
    public static void isLoginNeeded(CustomCatalog catalog) throws IOException {        
        if (catalog.getConf("hadoop.security.authentication").equalsIgnoreCase("kerberos")) {
            new KerberosAuthenticator(catalog).login();
        }
    }
}
