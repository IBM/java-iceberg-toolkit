/**
  * (c) Copyright IBM Corp. 2022. All Rights Reserved.
  */

package iceberg_cli.catalog;

import java.util.HashMap;
import java.util.Map;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.metastore.conf.MetastoreConf;

import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.databind.annotation.JsonDeserialize;

import iceberg_cli.utils.*;

@JsonDeserialize(using = CustomCatalogDeserializer.class)
public class CustomCatalog {
    private static final String DEFAULT_NAME = "default";
    private static final String HADOOP_PREFIX = "HADOOP_";
    private static final String HIVE_PREFIX = "HIVE_";
    private static final String METASTORE_PREFIX = "METASTORE_";
    private String name;
    private CatalogType type;
    public enum CatalogType {
        HIVE("org.apache.iceberg.hive.HiveCatalog"),
        HADOOP("org.apache.iceberg.hadoop.HadoopCatalog"),
        GLUE("org.apache.iceberg.aws.glue.GlueCatalog");

        public final String type;

        private CatalogType(String type) {
            this.type = type;
        }
    }
    private String metastoreUri;
    private Map<String, String> properties;
    private Configuration conf;

    public CustomCatalog() {
        this.name = DEFAULT_NAME;
        this.type = CatalogType.HIVE;
        this.properties = new HashMap<String, String>();
        this.conf = new HiveConf();
    }
    
    public CustomCatalog(String name) {
        this.name = name;
        this.type = CatalogType.HIVE;
        this.properties = new HashMap<String, String>();
        this.conf = new HiveConf();
    }

    public CustomCatalog(String name, CatalogType type) {
        this.name = name;
        this.type = type;
        this.properties = new HashMap<String, String>();
        this.conf = new HiveConf();
    }
    
    public CustomCatalog(String name, CatalogType type, String metastoreUri) {
        this.name = name;
        this.type = type;
        this.metastoreUri = metastoreUri;
        this.properties = new HashMap<String, String>();
        this.conf = new HiveConf();
    }

    public CustomCatalog(String name, CatalogType type, String metastoreUri, Map<String, String> props) {
        this.name = name;
        this.type = type;
        this.metastoreUri = metastoreUri;
        this.properties = props;
        this.conf = new HiveConf();
    }

    public CustomCatalog(String name, CatalogType type, String metastoreUri, Map<String, String> props, HiveConf conf) {
        this.name = name;
        this.type = type;
        this.metastoreUri = metastoreUri;
        this.properties = props;
        this.conf = conf;
    }
    
    public void updateUriAndWarehouse(String uri, String warehouse) {
        if (uri != null) {
            setConf(MetastoreConf.ConfVars.THRIFT_URIS, uri);
            setProperty("uri", uri);
        }
        if (warehouse != null) {
            setConf(MetastoreConf.ConfVars.WAREHOUSE, warehouse);
            setProperty("warehouse", warehouse);
        }
    }

    public String getName() {
        return name;
    }

    public CatalogType getType() {
        return type;
    }
    
    public String getMetastoreUri() {
        return metastoreUri;
    }

    @JsonIgnore
    public String getProperty(String key) {
        return properties.get(key);
    }
    
    public Configuration getConf() {
        return conf;
    }

    @JsonIgnore
    public String getConf(String key) {
        return MetastoreConf.get(conf, key);
    }
    
    @JsonIgnore
    public String getConf(MetastoreConf.ConfVars var) {
        return MetastoreConf.getVar(conf, var);
    }

    public Map<String, String> getProperties() {
        return properties;
    }

    public void setName(String name) {
        this.name = name;
    }

    public void setType(CatalogType type) {
        this.type = type;
    }
    
    public void setMetastoreUri(String metastoreUri) {
        this.metastoreUri = metastoreUri;
    }

    @JsonIgnore
    public void setProperty(String key, String value) {
        properties.put(key, value);
    }
    
    public void setConf(HiveConf conf) {
        this.conf = conf;
    }

    @JsonIgnore
    public void setConf(String key, String value) {
        conf.set(key, value);
    }
    
    @JsonIgnore
    public void setConf(HiveConf.ConfVars key, String value) {
        conf.set(key.varname, value);
    }
    
    @JsonIgnore
    public void setConf(MetastoreConf.ConfVars key, String value) {
        switch (key.getDefaultVal().getClass().getTypeName()) {
        case "java.lang.Double":
            MetastoreConf.setDoubleVar(conf, key, Double.valueOf(value));
            break;
        case "java.lang.Long":
            MetastoreConf.setLongVar(conf, key, Long.valueOf(value));
            break;
        case "java.lang.Boolean":
            MetastoreConf.setBoolVar(conf, key, Boolean.valueOf(value));
            break;
        default:
            MetastoreConf.setVar(conf, key, value);
        }
    }

    public void setProperties(Map<String, String> props) {
        this.properties = props;
    }

    public String toString() {
        StringBuilder sb = new StringBuilder();
        sb.append("Name: " + name);
        sb.append("\n");
        sb.append("Type: " + type);
        sb.append("\n");
        sb.append("Metastore Uri: " + metastoreUri);
        sb.append("\n");
        sb.append("Properties: " + properties);
        sb.append("\n");

        return sb.toString();
    }
    
    public void loadFromEnvironmentVariables() {
        Map<String, String> envVars = System.getenv();
        for (String var : envVars.keySet()) {
            // Override value if already set
            try { 
                if (var.startsWith(HIVE_PREFIX)) {
                    setConf(EnvVars.valueOf(var).hiveConf, envVars.get(var));
                } else if (var.startsWith(METASTORE_PREFIX)) {
                    setConf(EnvVars.valueOf(var).metaConf, envVars.get(var));
                } else {
                    setConf(EnvVars.valueOf(var).otherConf, envVars.get(var));
                }
            } catch (IllegalArgumentException e) {
             // Do nothing
            }
        }
    }
}
