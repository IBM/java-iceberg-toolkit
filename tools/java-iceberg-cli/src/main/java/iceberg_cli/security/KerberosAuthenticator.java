package iceberg_cli.security;

import java.io.IOException;

import org.apache.hadoop.hive.metastore.conf.MetastoreConf;
import org.apache.hadoop.security.UserGroupInformation;

import iceberg_cli.catalog.CustomCatalog;

public class KerberosAuthenticator {
    CustomCatalog catalog;
    String principal;
    String keytab;
    String ccache;
    
    public KerberosAuthenticator(CustomCatalog catalog) {
        this.catalog = catalog;
        getCredentials();
    }
    
    public void login() throws IOException {
        if (keytab == null) {
            loginUsingCcache();
        } else {
            loginUsingKeytab();
        }
    }
    
    public void getCredentials() {
        // Set principal
        principal = catalog.getConf(MetastoreConf.ConfVars.KERBEROS_PRINCIPAL);
        if (principal == null) {
            if (System.getenv("KRB5PRINCIPAL") != null) {
                principal = System.getenv("KRB5PRINCIPAL");
            }
        }
        
        // Set keytab
        keytab = catalog.getConf(MetastoreConf.ConfVars.KERBEROS_KEYTAB_FILE);
        if (keytab == null) {
            if (System.getenv("KRB5KEYTAB") != null) {
                keytab = System.getenv("KRB5KEYTAB");
            }
        }
        
        // Set ccache
        ccache = System.getenv("KRB5CCNAME");
            
    }
    
    public void loginUsingKeytab() throws IOException {
        UserGroupInformation.setConfiguration(catalog.getConf());
        UserGroupInformation.loginUserFromKeytab(principal, keytab);
    }
    
    public void loginUsingCcache() throws IOException {
        UserGroupInformation.setConfiguration(catalog.getConf());
        UserGroupInformation.getUGIFromTicketCache(ccache, principal);
    }
}
