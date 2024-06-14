package iceberg_cli.security;

import java.io.File;

import org.apache.hadoop.hive.metastore.conf.MetastoreConf;
import org.apache.hadoop.security.alias.CredentialProviderFactory;
import org.apache.hadoop.security.alias.JavaKeyStoreProvider;

import iceberg_cli.catalog.CustomCatalog;

public class AuthProvider {
    
    public static void isLoginNeeded(CustomCatalog catalog) throws Exception {
        if (catalog.getConf("hadoop.security.authentication").equalsIgnoreCase("kerberos")) {
            new KerberosAuthenticator(catalog).login();
        } else if (!catalog.getConf(MetastoreConf.ConfVars.METASTORE_CLIENT_PLAIN_USERNAME).isEmpty()) {
            new PlainAuthenticator(catalog).login();
        }
    }
    
    public static void clearSession(CustomCatalog catalog) {
        String providerPath = catalog.getConf(CredentialProviderFactory.CREDENTIAL_PROVIDER_PATH);
        if (providerPath != null) {
            String[] path = providerPath.split(JavaKeyStoreProvider.SCHEME_NAME + "://file");
            if (path.length == 2) {
                File file = new File(path[1]);
                if (file.exists() && file.isFile())
                    file.delete();
            }
        }
    }
}
