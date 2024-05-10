package iceberg_cli.catalog;

import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.metastore.conf.MetastoreConf;

import static org.apache.iceberg.CatalogProperties.IO_MANIFEST_CACHE_ENABLED;
import static org.apache.iceberg.CatalogProperties.IO_MANIFEST_CACHE_EXPIRATION_INTERVAL_MS;

public enum EnvVars {
    HADOOP_AUTHENTICATION ("hadoop.security.authentication"),
    ICEBERG_MANIFEST_CACHE_ENABLED (IO_MANIFEST_CACHE_ENABLED),
    ICEBERG_MANIFEST_CACHE_EXPIRATION_INTERVAL_MS (IO_MANIFEST_CACHE_EXPIRATION_INTERVAL_MS),
    METASTORE_URI (MetastoreConf.ConfVars.THRIFT_URIS),
    METASTORE_WAREHOUSE (MetastoreConf.ConfVars.WAREHOUSE),
    METASTORE_SASL_ENABLED (MetastoreConf.ConfVars.USE_THRIFT_SASL),
    METASTORE_KERBEROS_PRINCIPLE (MetastoreConf.ConfVars.KERBEROS_PRINCIPAL),
    METASTORE_KERBEROS_KEYTAB (MetastoreConf.ConfVars.KERBEROS_KEYTAB_FILE),
    METASTORE_SSL_ENABLED (MetastoreConf.ConfVars.USE_SSL),
    METASTORE_KEYSTORE_PATH (MetastoreConf.ConfVars.SSL_KEYSTORE_PATH),
    METASTORE_KEYSTORE_PASSWORD (MetastoreConf.ConfVars.SSL_KEYSTORE_PASSWORD),
    METASTORE_TRUSTSTORE_PATH (MetastoreConf.ConfVars.SSL_TRUSTSTORE_PATH),
    METASTORE_TRUSTSTORE_PASSWORD (MetastoreConf.ConfVars.SSL_TRUSTSTORE_PASSWORD),
    METASTORE_CLIENT_PLAIN_USERNAME (MetastoreConf.ConfVars.METASTORE_CLIENT_PLAIN_USERNAME);
    
    public final HiveConf.ConfVars hiveConf;
    public final MetastoreConf.ConfVars metaConf;
    public final String otherConf;
    
    private EnvVars(MetastoreConf.ConfVars var) {
        this.metaConf = var;
        this.hiveConf = null;
        this.otherConf = null;
    }
    
    private EnvVars(String var) {
        this.metaConf = null;
        this.hiveConf = null;
        this.otherConf = var;
    } 
    
    private EnvVars(HiveConf.ConfVars var) {
        this.metaConf = null;
        this.hiveConf = var;
        this.otherConf = null;
    }
}
