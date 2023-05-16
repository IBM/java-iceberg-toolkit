/**
 * (c) Copyright IBM Corp. 2022. All Rights Reserved.
 */

package iceberg.cli.commands;

import java.util.ArrayList;
import java.util.List;

public class Command {
    private String m_name;
    private String m_description;
    private List<Parameter> m_options;
    private List<Parameter> m_arguments;
    
    public Command(String name) {
        m_name = name;
        m_description = null;
        m_options = new ArrayList<Parameter>();
        m_arguments = new ArrayList<Parameter>();
    }
    
    public Command(String name, String description) {
        m_name = name;
        m_description = description;
        m_options = new ArrayList<Parameter>();
        m_arguments = new ArrayList<Parameter>();
    }
    
    public void setName(String name) {
        m_name = name;
    }
    
    public void setDescription(String description) {
        m_description = description;
    }
    
    public void addOption(String name) {
        m_options.add(new Parameter(name));
    }
    
    public void addOption(String name, String description) {
        m_options.add(new Parameter(name, description));
    }
    
    public void addOption(String name, String description, boolean isRequired) {
        m_options.add(new Parameter(name, description, isRequired));
    }
    
    public void addArgument(String name) {
        m_arguments.add(new Parameter(name));
    }
    
    public void addArgument(String name, String description) {
        m_arguments.add(new Parameter(name, description));
    }
    
    public void addArgument(String name, String description, boolean isRequired) {
        m_arguments.add(new Parameter(name, description, isRequired));
    }
    
    public String name() {
        return m_name;
    }
    
    public String description() {
        return m_description;
    }
    
    public List<Parameter> options() {
        return m_options;
    }
    
    public List<Parameter> arguments() {
        return m_arguments;
    }
    
    public List<Parameter> requiredArguments() {
        List<Parameter> requiredArgs = new ArrayList<Parameter>();
        for (Parameter arg : m_arguments) {
            if (arg.isRequired())
                requiredArgs.add(arg);
        }
        return requiredArgs;
    }
    
    public String toString() {
        String info = new String();
        
        if (m_description != null)
            info += String.format("\n%s\n" , m_description);

        if (m_options.size() > 0) {
            info += "\nOptions:\n";
            for (Parameter option : m_options) {
                info += String.format("%2s%-20s %s\n", " ", option.name(), option.description());
            }
        }

        if (m_arguments.size() > 0) {
            info += "\nPositional Arguments:\n";
            for (Parameter arg : m_arguments) {
                info += String.format("%2s%-20s %s\n", " ", arg.name(), arg.description());
            }
        }
        
        return info;
    }
}
