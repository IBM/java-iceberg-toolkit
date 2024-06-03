package iceberg_cli.security;

import java.io.File;
import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hive.metastore.conf.MetastoreConf;
import org.apache.hadoop.security.alias.CredentialProvider;
import org.apache.hadoop.security.alias.CredentialProvider.CredentialEntry;
import org.apache.hadoop.security.alias.CredentialProviderFactory;
import org.apache.hadoop.security.alias.JavaKeyStoreProvider;

import iceberg_cli.catalog.CustomCatalog;

public class PlainAuthenticator {
    CustomCatalog catalog;
    String username;
    String password;
    private static final String authMode = "PLAIN";
    private static final String passwordEnvVar = "METASTORE_CLIENT_PLAIN_PASSWORD";
    private static final String passwordConfVar = "hive.metastore.client.plain.password";
    private static final String dataDir = System.getProperty("java.io.tmpdir") + File.separator + "JavaHmsClient";
    
    public PlainAuthenticator(CustomCatalog catalog) throws Exception {
        this.catalog = catalog;
        getCredentials();
    }
    
    public void login() throws IOException {
        catalog.setConf(MetastoreConf.ConfVars.USE_THRIFT_SASL, "false");
        catalog.setConf(MetastoreConf.ConfVars.METASTORE_CLIENT_AUTH_MODE, authMode);
        catalog.setConf(MetastoreConf.ConfVars.EXECUTE_SET_UGI, "false");
        
        String fileName = String.format("hms_auth.%s", JavaKeyStoreProvider.SCHEME_NAME);
        String credUrl = JavaKeyStoreProvider.SCHEME_NAME + "://file" + dataDir + File.separator + fileName;
        validateCredFile(credUrl);
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
    
    private synchronized void validateCredFile(String credUrl) throws IOException {
        Configuration credConf = new Configuration();
        credConf.set(CredentialProviderFactory.CREDENTIAL_PROVIDER_PATH, credUrl);
        CredentialProvider provider = CredentialProviderFactory.getProviders(credConf).get(0);
   
       // Check if a credential entry exists for the provided username
       CredentialEntry entry = provider.getCredentialEntry(username);
       if (entry == null) {
           provider.createCredentialEntry(username, password.toCharArray());
           provider.flush();
       } else if (!(String.valueOf(entry.getCredential()).equals(password))) {
           // Check if the entry token is not the same as the provided token
           // for a username then, recreate an entry. This can happen as a
           // user can have multiple tokens.
           provider.deleteCredentialEntry(username);
           provider.createCredentialEntry(username, password.toCharArray());
           provider.flush();
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
}
