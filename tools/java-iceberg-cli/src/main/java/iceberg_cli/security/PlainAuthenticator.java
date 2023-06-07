package iceberg_cli.security;

import java.io.File;
import java.io.IOException;

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
    private static final String authMode = "PLAIN";
    private static final String passwordEnvVar = "METASTORE_CLIENT_PLAIN_PASSWORD";
    private static final String passwordConfVar = "hive.metastore.client.plain.password";
    private static final String testDataDir = System.getProperty("java.io.tmpdir") + File.separator + "JavaHmsClient";
    
    public PlainAuthenticator(CustomCatalog catalog) throws Exception {
        this.catalog = catalog;
        getCredentials();
    }
    
    
    public void login() throws IOException {
        catalog.setConf(MetastoreConf.ConfVars.USE_THRIFT_SASL, "false");
        catalog.setConf(MetastoreConf.ConfVars.METASTORE_CLIENT_AUTH_MODE, authMode);
        catalog.setConf(MetastoreConf.ConfVars.EXECUTE_SET_UGI, "false");
        
        String credUrl = createCredFile();
        catalog.setConf(CredentialProviderFactory.CREDENTIAL_PROVIDER_PATH, credUrl);
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
        String fileName = "hms_auth_" + username + "_" + password + "." + JavaKeyStoreProvider.SCHEME_NAME;
        String credUrl = JavaKeyStoreProvider.SCHEME_NAME + "://file" + testDataDir + File.separator + fileName;
        Configuration credConf = new Configuration();
        credConf.set(CredentialProviderFactory.CREDENTIAL_PROVIDER_PATH, credUrl);
        CredentialProvider provider = CredentialProviderFactory.getProviders(credConf).get(0);
    
        // Check if the credential entry already exists
        if (provider.getCredentialEntry(username) == null) {
            provider.createCredentialEntry(username, password.toCharArray());
            provider.flush();
        }
    
        return credUrl;
    }
}
