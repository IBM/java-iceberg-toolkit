/**
  * (c) Copyright IBM Corp. 2022. All Rights Reserved.
  */

package iceberg_cli.catalog;

import java.io.File;
import java.io.IOException;
import java.util.List;

import org.apache.commons.cli.ParseException;
import org.apache.hadoop.hive.metastore.conf.MetastoreConf;

import com.fasterxml.jackson.core.JsonParseException;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.core.exc.StreamWriteException;
import com.fasterxml.jackson.databind.DatabindException;
import com.fasterxml.jackson.databind.JsonMappingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.SerializationFeature;
import com.fasterxml.jackson.dataformat.yaml.YAMLFactory;
import com.fasterxml.jackson.dataformat.yaml.YAMLGenerator.Feature;
import com.fasterxml.jackson.dataformat.yaml.YAMLMapper;
import com.fasterxml.jackson.datatype.jsr310.JavaTimeModule;

import iceberg_cli.security.AuthProvider;
import iceberg_cli.utils.Credentials;

public class ConfigLoader {
    private static final String DEFAULT_CATALOG = "default";
    public static final String CONFIG_YML =
            String.format("%s/.java_iceberg_cli.yaml", System.getProperty("user.home"));
    
    /**
     * Read catalogs information from ~/.java_iceberg_cli.yaml
     * @return Catalogs which stores list of catalogs found in the file
     * @throws IOException
     */
    private static Catalogs readFromYaml() {
        try {
            ObjectMapper mapper =  new ObjectMapper(new YAMLFactory());
            mapper.findAndRegisterModules();
            mapper.registerModule(new JavaTimeModule());
            return mapper.readValue(new File(CONFIG_YML), Catalogs.class);
        } catch (Exception e) {
           System.err.println("WARNING: " + e.getMessage()); 
        }
        return new Catalogs();
    }

    /**
     * Load catalog information from the config file, environment variables, and user specified values.
     * Configuration values will be loaded as following, where the values can be overridden in the subsequent steps:
     * 1- Load from CONFIG_YML
     * 2- Load from environment variables
     * 3- User defined values
     * @param catalogName
     * @param uri
     * @param warehouse
     * @return Catalog
     * @throws Exception
     */
    public CustomCatalog init(String catalogName, String uri, String warehouse) throws Exception {
        CustomCatalog catalog = null;
        if (catalogName == null)
            catalogName = DEFAULT_CATALOG;
        
        // Use config file if exists
        if (new File(CONFIG_YML).isFile()) {
            catalog = loadCatalogFromConfig(catalogName);
        }
                
        if (catalog == null) {
            catalog = new CustomCatalog(catalogName);
        }
                
        // Load Hadoop configuration values from environment variables
        catalog.loadFromEnvironmentVariables();

        // Overwrite catalog configuration and properties, if specified by the user
        if (uri != null) {
            catalog.setConf(MetastoreConf.ConfVars.THRIFT_URIS, uri);
            catalog.setProperty("uri", uri);
        }
        if (warehouse != null) {
            catalog.setConf(MetastoreConf.ConfVars.WAREHOUSE, warehouse);
            catalog.setProperty("warehouse", warehouse);
        }
        
        // Authenticate with Kerberos, if needed
        AuthProvider.isLoginNeeded(catalog);

        return catalog;
    }

    /**
     * Get user specified catalog information from the config file
     * @param catalogName
     * @return Catalog
     * @throws IOException
     */
    public static CustomCatalog loadCatalogFromConfig(String catalogName) throws IOException {
        Catalogs catalogs = readFromYaml();
        return catalogs.getCatalog(catalogName);
    }

    /**
     * Write catalogs information to the config file
     * @param catalogs
     * @throws StreamWriteException
     * @throws DatabindException
     * @throws IOException
     */
    public static void writeToYaml(Catalogs catalogs) throws StreamWriteException, DatabindException, IOException {
        ObjectMapper mapper =  new YAMLMapper();
        mapper = new ObjectMapper(new YAMLFactory().disable(Feature.WRITE_DOC_START_MARKER));
        mapper.configure(SerializationFeature.FAIL_ON_EMPTY_BEANS, false);
        mapper.registerModule(new JavaTimeModule());
        mapper.writeValue(new File(CONFIG_YML), catalogs);
    }

    /**
     * Load a catalog from a JSON string
     * @param jsonString
     * @return Catalog
     * @throws JsonMappingException
     * @throws JsonProcessingException
     */
    public static CustomCatalog readFromJsonString(String jsonString) throws JsonMappingException, JsonProcessingException {
        ObjectMapper mapper = new ObjectMapper();
        return mapper.readValue(jsonString, CustomCatalog.class);
    }

    /**
     * Return a JSON string representing catalog information
     * @param catalog
     * @return String catalog information as a JSON string
     * @throws JsonProcessingException
     */
    public static String writeAsJsonString(CustomCatalog catalog) throws JsonProcessingException {
        ObjectMapper mapper = new ObjectMapper();
        mapper.configure(SerializationFeature.FAIL_ON_EMPTY_BEANS, false);
        mapper.registerModule(new JavaTimeModule());
        return mapper.writeValueAsString(catalog);
    }
}
