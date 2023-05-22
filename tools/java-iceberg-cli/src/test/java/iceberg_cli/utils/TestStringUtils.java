package iceberg_cli.utils;

import java.util.List;
import java.util.stream.Stream;

import javax.servlet.ServletException;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;
import org.junit.jupiter.params.provider.NullAndEmptySource;

public class TestStringUtils {
    
    public static Stream<Arguments> parameters() {
        return Stream.of(
            Arguments.of("a b c", new String[] {"a", "b", "c"}),
            Arguments.of("a 'b c' d", new String[] {"a", "b c", "d"}),
            Arguments.of("a \"b c\" d", new String[] {"a", "b c", "d"}),
            Arguments.of("a \"'b c' g 'h'\" d", new String[] {"a", "'b c' g 'h'", "d"}),
            Arguments.of("-w \"/hive/warehouse\" -t \"Test table\" -n test -a write --record '{\"records\":[{\"c1\":8, \"c2\": \"The quick brown fox jumps over the lazy dog.\"}]}'",
                    new String[] {"-w", "/hive/warehouse", "-t", "Test table", "-n", "test", "-a", "write", "--record", "{\"records\":[{\"c1\":8, \"c2\": \"The quick brown fox jumps over the lazy dog.\"}]}"}),
            Arguments.of("--record '{\"records\":[{\"c1\":8, \"c2\": \"The quick brown fox doesn\\'t jump over the lazy dog.\"}]}'",
                    new String[] {"--record", "{\"records\":[{\"c1\":8, \"c2\": \"The quick brown fox doesn\\'t jump over the lazy dog.\"}]}"}),
            Arguments.of("--record \"The quick brown fox doesn\\\"t jump over the lazy dog.\"",
                    new String[] {"--record", "The quick brown fox doesn\\\"t jump over the lazy dog."})
        );
    }
    
    public static Stream<Arguments> invalidParameters() {
        return Stream.of(
                Arguments.of("a 'b c d"),
                Arguments.of("a 'bc'e f"),
                Arguments.of("-w \"/hive/warehouse\" -t \"Test table\" -n test -a write --record \"{\"records\":[{\"c1\":8, \"c2\": \"The quick brown fox jumps over the lazy dog.\"}]}\""),
                Arguments.of("--record '{\"records\":[{\"c1\":8, \"c2\": \"The quick brown fox doesn\'t jump over the lazy dog.\"}]}'"),
                Arguments.of("--record \"The quick brown fox doesn\"t jump over the lazy dog.\"")
                );
    }
    
    @ParameterizedTest
    @MethodSource("parameters")
    public void testTokenizeQuotedString(String input, String[] expected) throws ServletException {
        try {
            List<String> actualTokens = StringUtils.tokenizeQuotedString(input);
            Assertions.assertArrayEquals(expected, actualTokens.toArray());
        } catch (Throwable t) {
            throw new ServletException("Error: " + t.getMessage(), t);
        }
    }
    
    @ParameterizedTest
    @NullAndEmptySource
    public void testTokenizeQuotedStringForNullAndEmptyString(String input) throws ServletException {
        try {
            List<String> actualTokens = StringUtils.tokenizeQuotedString(input);
            Assertions.assertArrayEquals(new String[] {}, actualTokens.toArray());
        } catch (Throwable t) {
            throw new ServletException("Error: " + t.getMessage(), t);
        }
    }
    
    @ParameterizedTest
    @MethodSource("invalidParameters")
    public void testTokenizeQuotedStringForInvalidString(String input) throws ServletException {
        try {
            Exception exception = Assertions.assertThrows(IllegalArgumentException.class, () -> {
                StringUtils.tokenizeQuotedString(input);
            });
            
            String expectedMessage = "Invalid message string";
            String actualMessage = exception.getMessage();
            
            Assertions.assertEquals(expectedMessage, actualMessage);
            
        } catch (Throwable t) {
            throw new ServletException("Error: " + t.getMessage(), t);
        }
    }
}
