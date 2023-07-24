package iceberg_cli.catalog;

import java.io.IOException;
import java.util.Map;
import java.util.Map.Entry;

import org.apache.hadoop.hive.conf.HiveConf;

import com.fasterxml.jackson.core.JacksonException;
import com.fasterxml.jackson.core.JsonParser;
import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.DeserializationContext;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.deser.std.StdDeserializer;

public class CustomCatalogDeserializer extends StdDeserializer<CustomCatalog> {
    
    private static final long serialVersionUID = 1L;

    public CustomCatalogDeserializer() {
        this(null);
    }

    public CustomCatalogDeserializer(Class<?> vc) {
        super(vc);
    }

    @Override
    public CustomCatalog deserialize(JsonParser p, DeserializationContext ctxt) throws IOException, JacksonException {
        JsonNode node = p.getCodec().readTree(p);
        String name = node.get("name").asText();
        CustomCatalog.CatalogType type = CustomCatalog.CatalogType.valueOf(node.get("type").asText());
        String metastoreUri = node.get("metastoreUri").asText();
        ObjectMapper mapper = new ObjectMapper();
        Map<String, String> properties = mapper.convertValue(node.get("properties"), new TypeReference<Map<String, String>>(){});
        Map<String, String> conf = mapper.convertValue(node.get("conf"), new TypeReference<Map<String, String>>(){});
        HiveConf hiveConf = new HiveConf();
        for (Entry<String, String> entry : conf.entrySet()) {
            hiveConf.set(entry.getKey(), entry.getValue());
        }
        
        return new CustomCatalog(name, type, metastoreUri, properties, hiveConf);
    }

}
