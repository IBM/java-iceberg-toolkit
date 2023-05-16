/**
 * (c) Copyright IBM Corp. 2022. All Rights Reserved.
 */

package iceberg_cli.utils;

import org.json.JSONObject;

public class Credentials
{
    protected JSONObject values;
    protected CredentialType type;
    public enum CredentialType {
        AWS
    }
    
    public Credentials(JSONObject values, String type) {
        // Get credentials
        this.values = values;
        // Get type
        this.type = Credentials.CredentialType.valueOf(type.toUpperCase());
        // Try getting credentials from environment variables
        this.checkEnvironmentVariables();
    }
    
    public void checkEnvironmentVariables() {
        if (getValue("ENDPOINT") == null)
            setValue("ENDPOINT", System.getenv("ENDPOINT"));
    }
    
    public String getValue(String key) {
        if (values.has(key))
            return (String) values.get(key);
        else
            return null;
    }
    
    public void setValue(String key, String value) {
        values.put(key, value);
    }
    
    public CredentialType getType() {
        return type;
    }
    
    public boolean isValid() {
        return (values.length() > 0);
    }
}
