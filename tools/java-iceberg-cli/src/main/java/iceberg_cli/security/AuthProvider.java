package iceberg_cli.security;

import org.apache.hadoop.hive.metastore.conf.MetastoreConf;

import iceberg_cli.catalog.CustomCatalog;

public class AuthProvider {
    
    public static void isLoginNeeded(CustomCatalog catalog) throws Exception {
        if (catalog.getConf("hadoop.security.authentication").equalsIgnoreCase("kerberos")) {
            new KerberosAuthenticator(catalog).login();
        } else if (!catalog.getConf(MetastoreConf.ConfVars.METASTORE_CLIENT_PLAIN_USERNAME).isEmpty()) {
            new PlainAuthenticator(catalog).login();
        }
    }
}
