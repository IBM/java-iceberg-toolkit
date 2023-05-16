/**
 * (c) Copyright IBM Corp. 2022. All Rights Reserved.
 */

package iceberg_cli.utils;

import org.apache.commons.cli.ParseException;
import org.json.JSONObject;

public class AwsCredentials extends Credentials {

    public AwsCredentials(JSONObject values) throws ParseException {
        super(values, "AWS");
        
        // Try getting credentials from environment variables
        fromEnvironmentVariables();
        
        // Set default region
        if (getValue("AWS_REGION") == null)
            values.put("AWS_REGION", "us-east-1");
    }
    
    public void fromEnvironmentVariables() {
        if (getValue("AWS_ACCESS_KEY_ID") == null)
            setValue("AWS_ACCESS_KEY_ID", System.getenv("AWS_ACCESS_KEY_ID"));
        
        if (getValue("AWS_SECRET_ACCESS_KEY") == null)
            setValue("AWS_SECRET_ACCESS_KEY", System.getenv("AWS_SECRET_ACCESS_KEY"));
        
        if (getValue("AWS_REGION") == null)
            setValue("AWS_REGION", System.getenv("AWS_REGION"));
        
        if (getValue("AWS_SESSION_TOKEN") == null)
            setValue("AWS_SESSION_TOKEN", System.getenv("AWS_SESSION_TOKEN"));
    }
    
    public boolean isValid() {
        return (getValue("AWS_ACCESS_KEY_ID") != null && getValue("AWS_SECRET_ACCESS_KEY") != null);
    }

}
