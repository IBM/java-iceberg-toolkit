/**
 * (c) Copyright IBM Corp. 2022. All Rights Reserved.
 */

package iceberg.cli;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.commons.cli.*;

import iceberg.cli.commands.Command;
import iceberg.cli.commands.Parameter;

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
        switch (subCommand) {
            case "list":
                break;
            case "describe":
            case "drop":
            case "create":
                validateNamespace();
                break;
            case "files":
            case "snapshot":
            case "read":
            case "schema":
            case "spec":
            case "uuid":
            case "rename":
            case "commit":
            case "write":
            case "location":
            case "type":
                validateTable();
                break;
            case "default":
                System.err.println("Invalid command");
        }
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
            m_namespace = levels[0];
            m_table = levels[1];
        } else {
            m_namespace = identifier;
        }
    }

    private void validateNamespace() throws ParseException {
        if (m_namespace == null)
            throw new ParseException("Missing identifier");
    }

    private void validateTable() throws ParseException {
        if (m_namespace == null || m_table == null)
            throw new ParseException("Missing identifier");
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
