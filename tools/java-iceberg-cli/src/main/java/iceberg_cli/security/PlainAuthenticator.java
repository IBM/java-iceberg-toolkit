package iceberg_cli.security;

import java.io.File;
import java.io.IOException;
import java.util.UUID;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hive.metastore.conf.MetastoreConf;
import org.apache.hadoop.security.alias.CredentialProvider;
import org.apache.hadoop.security.alias.CredentialProviderFactory;
import org.apache.hadoop.security.alias.JavaKeyStoreProvider;

import iceberg_cli.catalog.CustomCatalog;

public class PlainAuthenticator {
    CustomCatalog catalog;
    String username;
    String password;
    String keystore;
    private static final String authMode = "PLAIN";
    private static final String passwordEnvVar = "METASTORE_CLIENT_PLAIN_PASSWORD";
    private static final String passwordConfVar = "hive.metastore.client.plain.password";
    private static final String dataDir = System.getProperty("java.io.tmpdir") + File.separator + "JavaHmsClient";
    
    public PlainAuthenticator(CustomCatalog catalog) throws Exception {
        this.catalog = catalog;
        getCredentials();
        keystore = String.format("hms_auth_%s.%s", UUID.randomUUID().toString(), JavaKeyStoreProvider.SCHEME_NAME);
    }
    
    public void login() throws IOException {
        catalog.setConf(MetastoreConf.ConfVars.USE_THRIFT_SASL, "false");
        catalog.setConf(MetastoreConf.ConfVars.METASTORE_CLIENT_AUTH_MODE, authMode);
        catalog.setConf(MetastoreConf.ConfVars.EXECUTE_SET_UGI, "false");
        
        String credUrl = createCredFile();
        catalog.setConf(CredentialProviderFactory.CREDENTIAL_PROVIDER_PATH, credUrl);
    }
    
    public static void cleanup() {
        File dir = new File(dataDir);
        if (dir.exists()) {
            for (String file : dir.list()) {
                new File(dataDir, file).delete();
            }
        }
    }
    
    private void getCredentials() throws Exception {
        username = catalog.getConf(MetastoreConf.ConfVars.METASTORE_CLIENT_PLAIN_USERNAME);
        if (username == null)
            throw new Exception("Username is required for plain authentication");
        
        password = catalog.getConf(passwordConfVar);
        if (password == null)
            password = System.getenv(passwordEnvVar);
        
        // Validate password
        if (password == null)
            throw new Exception("Password is required for plain authentication");
    }
    
    private String createCredFile() throws IOException {
        String credUrl = JavaKeyStoreProvider.SCHEME_NAME + "://file" + dataDir + File.separator + keystore;
        Configuration credConf = new Configuration();
        credConf.set(CredentialProviderFactory.CREDENTIAL_PROVIDER_PATH, credUrl);
        CredentialProvider provider = CredentialProviderFactory.getProviders(credConf).get(0);
        
        // Cleanup if keystore already exists
        File file = new File(dataDir, keystore);
        if (file.exists() && file.isFile())
            file.delete();
    
        // Create a credential entry for the provided username
        provider.createCredentialEntry(username, password.toCharArray());
        provider.flush();
    
        return credUrl;
    }
}
