package iceberg_cli;

import java.util.stream.Stream;

import javax.servlet.ServletException;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

import iceberg_cli.utils.CliLogger;

public class TestCustomLogger {
    public static Stream<Arguments> parameters() {
        return Stream.of(
            // test creds and conf masking
            Arguments.of("--creds '{\"AWS_ACCESS_KEY_ID\":\"ID\",\"AWS_SECRET_ACCESS_KEY\":\"KEY\"}'", "--creds '####'"),
            Arguments.of("-c '{\"AWS_ACCESS_KEY_ID\":\"ID\",\"AWS_SECRET_ACCESS_KEY\":\"KEY\"}'", "-c '####'"),
            Arguments.of("--conf '{\"hive.metastore.plain.username\":\"username\"}'", "--conf '####'"),
            Arguments.of("--creds, '{\"AWS_ACCESS_KEY_ID\":\"ID\",\"AWS_SECRET_ACCESS_KEY\":\"KEY\"}'", "--creds, '####'"),
            Arguments.of("-c, '{\"AWS_ACCESS_KEY_ID\":\"ID\",\"AWS_SECRET_ACCESS_KEY\":\"KEY\"}'", "-c, '####'"),
            Arguments.of("--conf, '{\"hive.metastore.plain.username\":\"username\"}'", "--conf, '####'"),
            Arguments.of("--creds'{\"AWS_ACCESS_KEY_ID\":\"ID\",\"AWS_SECRET_ACCESS_KEY\":\"KEY\"}'", "--creds'####'"),
            Arguments.of("-c'{\"AWS_ACCESS_KEY_ID\":\"ID\",\"AWS_SECRET_ACCESS_KEY\":\"KEY\"}'", "-c'####'"),
            Arguments.of("--conf'{\"hive.metastore.plain.username\":\"username\"}'", "--conf'####'"),
            Arguments.of("--creds \"{\"AWS_ACCESS_KEY_ID\":\"ID\",\"AWS_SECRET_ACCESS_KEY\":\"KEY\"}\"", "--creds '####'"),
            Arguments.of("-c \"{\"AWS_ACCESS_KEY_ID\":\"ID\",\"AWS_SECRET_ACCESS_KEY\":\"KEY\"}\"", "-c '####'"),
            Arguments.of("--conf \"{\"hive.metastore.plain.username\":\"username\"}\"", "--conf '####'"),
            
            // test keywords masking
            Arguments.of("\"AWS_ACCESS_KEY_ID\":\"ID\"", "\"AWS_ACCESS_KEY_ID\":'####'"),
            Arguments.of("\"AWS_ACCESS_KEY_ID\" :\"ID\"", "\"AWS_ACCESS_KEY_ID\" :'####'"),
            Arguments.of("\"AWS_ACCESS_KEY_ID\" : \"ID\"", "\"AWS_ACCESS_KEY_ID\" :'####'"),
            Arguments.of("'AWS_ACCESS_KEY_ID':\"ID\"", "'AWS_ACCESS_KEY_ID':'####'"),
            Arguments.of("\"AZURE_ACCOUNT_KEY\":\"KEY\"", "\"AZURE_ACCOUNT_KEY\":'####'"),
            
            // test case sensitive keywords masking
            Arguments.of("'aws_access_key_id':\"ID\"", "'aws_access_key_id':'####'"),
            Arguments.of("'azure_client_id':\"ID\"", "'azure_client_id':'####'"),
            Arguments.of("'azure_sas_token':\"token\"", "'azure_sas_token':'####'"),
            Arguments.of("'TOKEN':\"sometoken\"", "'TOKEN':'####'")
        );
    }
    
    @ParameterizedTest
    @MethodSource("parameters")
    public void testTokenizeQuotedString(String input, String expected) throws ServletException {
        try {
            String maskedMessage = CliLogger.maskMessage(input);
            Assertions.assertEquals(expected, maskedMessage);
        } catch (Throwable t) {
            throw new ServletException("Error: " + t.getMessage(), t);
        }
    }

}

