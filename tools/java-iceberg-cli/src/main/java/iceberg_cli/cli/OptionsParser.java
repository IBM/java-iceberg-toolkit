/**
 * (c) Copyright IBM Corp. 2022. All Rights Reserved.
 */

package iceberg_cli.cli;

import java.util.Map;

import org.apache.commons.cli.*;

import iceberg_cli.cli.commands.*;

public class OptionsParser {
    private String m_uri;
    private String m_warehouse;
    private String m_outputFormat;
    private String m_tableFormat;
    private String m_snapshotId;
    private String m_catalog;
    private String m_credentials;

    public OptionsParser() {
        m_uri = null;
        m_warehouse = null;
        m_outputFormat = "console";
        m_tableFormat = "iceberg";
        m_snapshotId = null;
        m_catalog = "default";
        m_credentials = null;
    }
    
    /**
     * Display usage if help option is passed
     * @param m_options
     */
    protected void displayHelp(Map<String, Command> commands, Options options) {
        HelpFormatter formatter = new HelpFormatter();
        formatter.printHelp("java -jar <jar_name> [options] command [args]", options);

        System.out.println("\nCommands:");
        for (Map.Entry<String, Command> entry : commands.entrySet()) {
            Command cmd = entry.getValue();
            System.out.println(String.format("%2s%-20s %s", " ", cmd.name(), cmd.description()));
        }
        
        System.exit(0);
    }
    
    /**
     * Parse options and return left over arguments (if any)
     * @param args
     * @return
     * @throws ParseException 
     */
    public void parseOptions(Map<String, Command> commands, String[] args) throws ParseException {
        Options options = new Options();
        options.addOption(Option.builder("h").longOpt("help").desc("Show this help message").build());
        options.addOption(Option.builder("c").longOpt("credential").argName("credentials").hasArg().desc("Supported credentials : AWS").build());
        options.addOption(Option.builder("u").longOpt("uri").argName("value").hasArg().desc("Hive metastore to use").build());
        options.addOption(Option.builder("w").longOpt("warehouse").argName("value").hasArg().desc("Table location").build());
        options.addOption(Option.builder("o").longOpt("output").argName("console|csv|json").hasArg().desc("Show output in this format").build());
        options.addOption(Option.builder().longOpt("catalog").argName("value").hasArg().desc("Read properties for this catalog from the config file").build());
        options.addOption(Option.builder("m").longOpt("format").argName("iceberg|hive").hasArg().desc("The format of the table we want to display").build());
        options.addOption(Option.builder().longOpt("snapshot").argName("snapshot ID").hasArg().desc("Snapshot ID to use").build());
        
        CommandLineParser parser = new DefaultParser();

        try {
            CommandLine cmd = parser.parse(options, args);

            if (cmd.hasOption("h")) {
                displayHelp(commands, options);
            } else {
                // Get metastore URI
                if (!cmd.hasOption("u"))
                    throw new ParseException("Missing required option: uri");
                m_uri = cmd.getOptionValue("u");
                // Get warehouse
                if (cmd.hasOption("w"))
                    m_warehouse = cmd.getOptionValue("w");
                // Get output format
                if (cmd.hasOption("o"))
                    m_outputFormat = cmd.getOptionValue("o");
                // Get table format
                if (cmd.hasOption("format"))
                    m_tableFormat = cmd.getOptionValue("format").toUpperCase();
                // Get snapshot id
                if (cmd.hasOption("snapshot"))
                    m_snapshotId = cmd.getOptionValue("snapshot");
                // Get credentials
                if (cmd.hasOption("credential"))
                    m_credentials = cmd.getOptionValue("credential");
                // Get catalog name
                if (cmd.hasOption("catalog"))
                    m_catalog = cmd.getOptionValue("catalog");
            }
         
        } catch (ParseException exp) {
            System.err.println("Error parsing options: " + exp.getMessage());
            System.exit(1);
        }
    }

    // Getter functions
    public String uri() { return m_uri; }
    public String warehouse() { return m_warehouse; }
    public String tableFormat() { return m_tableFormat; }
    public String outputFormat() { return m_outputFormat; }
    public String snapshotId() { return m_snapshotId; }
    public String credentials() { return m_credentials; }
    public String catalog() { return m_catalog; }
}
