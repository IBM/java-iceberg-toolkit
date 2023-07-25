/**
 * (c) Copyright IBM Corp. 2022. All Rights Reserved.
 */

package iceberg_cli.cli;

import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;

import org.apache.commons.cli.ParseException;

import iceberg_cli.cli.commands.Command;

public class Parser {
    private Map<String, Command> m_commands;
    private OptionsParser optParser;
    private SubcommandParser cmdParser;
    private Command m_command;

    public Parser() {
        initializeCommands();
        optParser = new OptionsParser();
        cmdParser = new SubcommandParser();
    }

    public void parseArguments(String[] args) throws ParseException {
        int index = subCommandIndex(args);
        assert index <= args.length;
        String[] options = Arrays.copyOfRange(args, 0, index);
        String[] cmdArguments = Arrays.copyOfRange(args, index, args.length);
        
        // Parse options 
        optParser.parseOptions(m_commands, options);

        // Get subcommand
        if (cmdArguments.length == 0)
            throw new ParseException("Error: Invalid operation");
        m_command = m_commands.get(cmdArguments[0]);

        // Parse subcommand options and arguments
        cmdParser.parseCommand(m_command, cmdArguments);
    }

    // Fetch command
    public String command() { return m_command.name(); }
    
    // Get OptionsParser
    public OptionsParser optParser() { return optParser; }

    // Get arguments
    public String outputFile() { return cmdParser.outputFile(); }
    public boolean overwrite() { return cmdParser.overwrite(); }
    public boolean fetchAll() { return cmdParser.fetchAll(); }
    public String namespace() { return cmdParser.namespace(); }
    public String table() { return cmdParser.table(); }
    public String getPositionalArg(String name) { return cmdParser.getPositionalArg(name); }

    private int subCommandIndex(String[] args) {
        int i = 0;
        for (; i < args.length; ++i) {
            if (args[i].startsWith("-") || args[i].startsWith("--") || !(m_commands.containsKey(args[i])))
                continue;
            return i;
        }
        return i;
    }
    
    private void initializeCommands() {
        m_commands = new HashMap<String, Command>();
        
        Command commit = new Command("commit", "Commit file(s) to a table");
        commit.addOption("--help", "Show this help message and exit");
        commit.addArgument("identifier", "Table identifier", true);
        commit.addArgument("data-files", "Data file(s) to commit", true);
        m_commands.put("commit", commit);
        
        Command create = new Command("create", "Create a table or a namespace");
        create.addOption("--help", "Show this help message and exit");
        create.addOption("--force", "If table exists, recreate an empty table");
        create.addArgument("identifier", "Table or namespace identifier", true);
        create.addArgument("schema", "Create a table using this schema");
        m_commands.put("create", create);
        
        Command describe = new Command("describe", "Get details of a table or a namespace");
        describe.addOption("--help", "Show this help message and exit");
        describe.addArgument("identifier", "Table or namespace identifier", true);
        m_commands.put("describe", describe);

        Command alter = new Command("alter", "Alter a table");
        alter.addOption("--help", "Show this help message and exit");
        alter.addArgument("identifier", "Table or namespace identifier", true);
        create.addArgument("schema", "Alter a table using this schema");
        m_commands.put("alter", alter);

        Command drop = new Command("drop", "Drop a table or a namespace");
        drop.addOption("--help", "Show this help message and exit");
        drop.addArgument("identifier", "Table or namespace identifier", true);
        m_commands.put("drop", drop);
        
        Command files = new Command("files", "List data files of a table");
        files.addOption("--help", "Show this help message and exit");
        files.addArgument("identifier", "Table identifier", true);
        m_commands.put("files", files);
        
        Command list = new Command("list", "List tables or namespaces");
        list.addOption("--help", "Show this help message and exit");
        list.addOption("--all", "Show tables in all namespaces");
        list.addArgument("identifier", "Table or namespace identifier");
        m_commands.put("list", list);
        
        Command location = new Command("location", "Fetch table location");
        location.addOption("--help", "Show this help message and exit");
        location.addArgument("identifier", "Table identifier", true);
        m_commands.put("location", location);
        
        Command metadata = new Command("metadata", "Get table metadata");
        metadata.addOption("--help", "Show this help message and exit");
        metadata.addArgument("identifier", "Table identifier", true);
        m_commands.put("metadata", metadata);
        
        Command recordcount = new Command("recordcount", "Get total number of records in a table");
        recordcount.addOption("--help", "Show this help message and exit");
        recordcount.addArgument("identifier", "Table identifier", true);
        m_commands.put("recordcount", recordcount);
        
        Command read = new Command("read", "Read from a table");
        read.addOption("--help", "Show this help message and exit");
        read.addArgument("identifier", "Table identifier", true);
        m_commands.put("read", read);
        
        Command rename = new Command("rename", "Rename a table a table");
        rename.addOption("--help", "Show this help message and exit");
        rename.addArgument("identifier", "Table identifier", true);
        rename.addArgument("to_identifier", "New table identifier", true);
        m_commands.put("rename", rename);
        
        Command rewrite = new Command("rewrite", "Rewrite (replace) file(s) in a table");
        rewrite.addOption("--help", "Show this help message and exit");
        rewrite.addArgument("identifier", "Table identifier", true);
        rewrite.addArgument("data-files", "Data file(s) to delete and data file(s) to add", true);
        m_commands.put("rewrite", rewrite);

        Command schema = new Command("schema", "Fetch schema of a table");
        schema.addOption("--help", "Show this help message and exit");
        schema.addArgument("identifier", "Table identifier", true);
        m_commands.put("schema", schema);
        
        Command snapshot = new Command("snapshot", "Fetch latest or all snapshot(s) of a table");
        snapshot.addOption("--help", "Show this help message and exit");
        snapshot.addOption("--all", "Show all snapshots");
        snapshot.addArgument("identifier", "Table identifier", true);
        m_commands.put("snapshot", snapshot);
        
        Command spec = new Command("spec", "Fetch partition spec of a table");
        spec.addOption("--help", "Show this help message and exit");
        spec.addArgument("identifier", "Table identifier", true);
        m_commands.put("spec", spec);
        
        Command tasks = new Command("tasks", "List scan tasks of a table");
        tasks.addOption("--help", "Show this help message and exit");
        tasks.addArgument("identifier", "Table identifier", true);
        m_commands.put("tasks", tasks);
        
        Command type = new Command("type", "Fetch table type");
        type.addOption("--help", "Show this help message and exit");
        type.addArgument("identifier", "Table identifier", true);
        m_commands.put("type", type);
        
        Command uuid = new Command("uuid", "Fetch uuid of a table");
        uuid.addOption("--help", "Show this help message and exit");
        uuid.addArgument("identifier", "Table identifier", true);
        m_commands.put("uuid", uuid);
        
        Command write = new Command("write", "Write to a table");
        write.addOption("--help", "Show this help message and exit");
        write.addOption("--output-file", "Output file location");
        write.addArgument("identifier", "Table identifier", true);
        write.addArgument("records", "Json string of records to write to a table", true);
        m_commands.put("write", write);
    }
}
