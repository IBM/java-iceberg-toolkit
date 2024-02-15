/**
 * (c) Copyright IBM Corp. 2022. All Rights Reserved.
 */

package iceberg_cli.cli;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.commons.cli.*;

import iceberg_cli.cli.commands.Command;
import iceberg_cli.cli.commands.Parameter;

public class SubcommandParser {
    private Map<String, String> m_positionalArgs;
    private String m_namespace;
    private String m_table;
    private String m_outputFile;
    private boolean m_force;
    private boolean m_allFlag;

    protected String[] parseOptions(Command command, String[] subCommand) {
        CommandLineParser parser = new DefaultParser();
        Options options = new Options();
        options.addOption(Option.builder("h").longOpt("help").desc("Show this help message").build());
        options.addOption(Option.builder("f").longOpt("force").desc("Overwrite an existing table").build());
        options.addOption(Option.builder("o").longOpt("output-file").argName("value").hasArg().desc("File location").build());
        options.addOption(Option.builder("a").longOpt("all").desc("Show all").build());

        try {
            CommandLine cmd = parser.parse(options, subCommand);

            if (cmd.hasOption("h")) {
                displayHelp(command);
            } else {
                if (cmd.hasOption("f")) m_force = true;
                if (cmd.hasOption("o")) m_outputFile = cmd.getOptionValue("o");
                if (cmd.hasOption("a")) m_allFlag = true;
                
                return cmd.getArgs();
            }
         
        } catch (ParseException exp) {
            System.err.println(exp.getMessage());
            System.exit(1);
        }
        
        return null;
    }

    protected void parseCommand(Command command, String[] args) throws ParseException{
        String[] remainingArgs = parseOptions(command, args);
        /// Check if action is missing
        if (remainingArgs.length < 1)
            throw new ParseException("Missing action");
        
        int index = 0;
        String subCommand = remainingArgs[index++];
        // Parse positional arguments
        m_positionalArgs = new HashMap<String, String>();
        parsePositionalArgs(command, remainingArgs, index);
        // Parse and validate identifier
        parseIdentifier();
    }

    /**
     * Display usage if help option is passed
     * @param command
     */
    private void displayHelp(Command command) {
        String usage = String.format("usage: java -jar <jar_name> [options] %s [options]", command.name());
        List<Parameter> args = command.requiredArguments();
        for (Parameter arg : args)
            usage += String.format(" %s", arg.name());
        System.out.println(usage);
        System.out.println(command);
        System.exit(0);
    }
    
    private void parseIdentifier() throws ParseException{
        String identifier = m_positionalArgs.get("identifier");
        
        if (identifier == null)
            return;
                        
        if (identifier.contains(".")) {
            String[] levels = identifier.split("\\.");
            if (levels.length < 2)
                throw new ParseException("Invalid identifier");
            else if (levels.length > 2) {
                String prev_str = null;
                String cur_str = null;
                String[] new_levels = new String[2];
                int new_level_idx = 0;
                int len;
                for (int i = 0; i < levels.length; i++) {
                    System.out.println("l:" + levels[i]);
                    if (prev_str != null) {
                        cur_str = prev_str + "." + levels[i];
                    } else {
                        cur_str = levels[i];
                    }
                    len = cur_str.length();
                    if (cur_str.charAt(len-1) == '\\')
                        prev_str = cur_str;
                    else {
                        new_levels[new_level_idx] = cur_str;
                        new_level_idx ++;
                        if (new_level_idx == 2 && (new_level_idx < (levels.length - 1)))
                            throw new ParseException("Invalid identifier");
                    }
                }
                /*
                if (new_levels.length < 2 || new_levels.length > 2)
                    throw new ParseException("Invalid identifier");
                */
                
                levels = new_levels;
            }
            m_namespace = levels[0];
            m_table = levels[1];
        } else {
            m_namespace = identifier;
        }
    }

    private void parsePositionalArgs(Command command, String[] subCommand, int startIndex) throws ParseException {
        for (Parameter param : command.arguments()) {
            String name = param.name();
            if (startIndex < subCommand.length)
                m_positionalArgs.put(name, subCommand[startIndex++]);
            else if (param.isRequired() && (m_positionalArgs.get(name) == null))
                throw new ParseException("Missing required field: " + name);
        }
    }
    
    // Getter functions
    public String outputFile() { return m_outputFile; }
    public boolean overwrite() { return m_force; }
    public boolean fetchAll() { return m_allFlag; }
    public String namespace() { return m_namespace; }
    public String table() { return m_table; }
    public String getPositionalArg(String name) { return m_positionalArgs.get(name); }
}
