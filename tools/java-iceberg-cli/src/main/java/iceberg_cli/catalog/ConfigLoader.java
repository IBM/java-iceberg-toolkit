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
    private static final String CONFIG_YML = ".java_iceberg_cli.yaml";
    
    /**
     * Read catalogs information from configuration file
     * @param configFile
     * @return Catalogs which stores list of catalogs found in the file
     * @throws IOException
     */
    private static Catalogs readFromYaml(String configFile) {
        try {
            File file = new File(configFile);
            if (file.isFile()) {
                ObjectMapper mapper =  new ObjectMapper(new YAMLFactory());
                mapper.findAndRegisterModules();
                mapper.registerModule(new JavaTimeModule());
                return mapper.readValue(file, Catalogs.class);
            }
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
        
        // Read from config file, if it exists
        catalog = loadCatalogFromConfig(catalogName, uri);
                
        if (catalog == null) {
            catalog = new CustomCatalog();
        }
                
        // Load from environment variables
        catalog.loadFromEnvironmentVariables();

        // Overwrite catalog configuration and properties, if specified by the user
        catalog.updateUriAndWarehouse(uri, warehouse);
        
        // Authenticate with Kerberos, if needed
        AuthProvider.isLoginNeeded(catalog);

        return catalog;
    }

    /**
     * Get user specified catalog information from the config file
     * @param catalogName
     * @param uri
     * @return Catalog
     * @throws IOException
     */
    public static CustomCatalog loadCatalogFromConfig(String catalogName, String uri) throws IOException {
        // Try loading config file from:
        // 1-ICEBERG_CONFIG
        // 2- User home directory
        String directory = System.getenv("ICEBERG_CONFIG");
        String configFile = String.format("%s/%s", directory, CONFIG_YML);
        File file = new File(configFile);
        if (directory == null || !file.isFile()) {
            directory = System.getProperty("user.home");
            configFile = String.format("%s/%s", directory, CONFIG_YML);
        }
        Catalogs catalogs = readFromYaml(configFile);
        // Load config values only when the metastore URI matches
        return catalogs.getCatalog(catalogName, uri); 
    }

    /**
     * Write catalogs information to the config file
     * @param configFile
     * @param catalogs
     * @throws StreamWriteException
     * @throws DatabindException
     * @throws IOException
     */
    public static void writeToYaml(String configFile, Catalogs catalogs) throws StreamWriteException, DatabindException, IOException {
        ObjectMapper mapper =  new YAMLMapper();
        mapper = new ObjectMapper(new YAMLFactory().disable(Feature.WRITE_DOC_START_MARKER));
        mapper.configure(SerializationFeature.FAIL_ON_EMPTY_BEANS, false);
        mapper.registerModule(new JavaTimeModule());
        mapper.writeValue(new File(configFile), catalogs);
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
