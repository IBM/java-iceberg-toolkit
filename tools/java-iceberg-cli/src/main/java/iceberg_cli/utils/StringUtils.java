/**
 * (c) Copyright IBM Corp. 2023. All Rights Reserved.
 */

package iceberg_cli.utils;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import org.apache.log4j.Logger;

public class StringUtils {
    private static final String BEGINNING_QUOTES = "^\"|^'";
    private static final String ENCLOSING_QUOTES = "^\"|\"$|^'|'$";
    /**
     * Given a string of quoted strings, split the string into a list of
     * arguments.
     * @param message
     * @return list of arguments
     * @throws IllegalArgumentException
     * @throws IOException
     * 
     * Steps:
     * 1- Get space delimited tokens
     * 2- Concatenate delimited tokens that are enclosed in quotes
     * 3- Returned final set of tokens
     * 
     * Examples:
     * "a abc d" => ["a", "abc", "d"]
     * "a \"ab c\" d" => ["a", "ab c", d"]
     * "a '\"abc\" 1' f" => ["a", "abc 1", "f"]
     */
    public static List<String> tokenizeQuotedString(String message) throws IllegalArgumentException, IOException {
        List<String> tokens = new ArrayList<String>();
        
        if (message == null || message.isEmpty())
            return tokens;
        
        // Split tokens on whitespace
        String[] delimitedTokens = message.trim().split(" ");
        
        String token = "", closingQuote = "";
        Boolean findClosingQuote = false, isFirst = false;
        // Concatenate space delimited tokens that are enclosed in quotes
        for (String delimitedToken : delimitedTokens) {
            if (!findClosingQuote) {
                if (delimitedToken.startsWith("'")) {
                    closingQuote = "'";
                    findClosingQuote = true;
                    isFirst = true;
                } else if (delimitedToken.startsWith("\"")) {
                    closingQuote = "\"";
                    findClosingQuote = true;
                    isFirst = true;
                } else {
                    tokens.add(delimitedToken.replaceAll(ENCLOSING_QUOTES, ""));
                }
            }
            
            if (findClosingQuote) {
                // There will be a starting quote if it's the first token in the enclosed string 
                if (isFirst) {
                    delimitedToken = delimitedToken.replaceAll(BEGINNING_QUOTES, "");
                    isFirst = false;
                }
                token = findToken(tokens, token, delimitedToken, closingQuote);
                findClosingQuote = !token.isEmpty();
            }
        }
        
        // All quotes should have been found by now
        if (findClosingQuote) {
            Logger log = CliLogger.getLogger();
            log.error("Invalid request, found mismatched quotes: " + message);
            throw new IllegalArgumentException("Invalid request, found mismatched quotes");
        }
        
        return tokens;
    }
    
    /**
     * Given delimited token, find the next occurrence of the required unescaped closing quote
     * @param tokens
     * @param token
     * @param delimitedToken
     * @param quote
     * @return concatenated tokens if delimited token didn't have the closing quote or empty string
     * if the closing quote was found. 
     */
    private static String findToken(List<String> tokens, String token, String delimitedToken, String quote) {
        token += " " + delimitedToken;
        int index = 0;
        while (index < delimitedToken.length()) {
            index = delimitedToken.indexOf(quote, index);
            if (index == -1)
                break;
            
            // Ignore if it's an escaped quote
            if (index > 0 && delimitedToken.charAt(index - 1) == '\\') {
                index++;
                continue;
            }
            
            // If the closing quote is not at the end of the token then, the input string is invalid.
            // Such as, a 'bc'e f
            if (index != delimitedToken.length() - 1)
                throw new IllegalArgumentException("Invalid request, found closing quote before end of the space delimited word");
            
            // Found the closing quote
            tokens.add(token.replaceAll(ENCLOSING_QUOTES, "").trim());
            token = "";
            break;
        }
        
        return token;
    }
}

