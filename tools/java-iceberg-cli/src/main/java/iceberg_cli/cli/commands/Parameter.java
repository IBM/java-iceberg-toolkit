/**
 * (c) Copyright IBM Corp. 2022. All Rights Reserved.
 */

package iceberg_cli.cli.commands;

public class Parameter {
    private String m_name;
    private String m_description;
    private boolean m_isRequired;
    
    public Parameter(String name) {
        m_name = name;
        m_description = null;
        m_isRequired = false;
    }

    public Parameter(String name, String description) {
        m_name = name;
        m_description = description;
        m_isRequired = false;
    }
    
    public Parameter(String name, String description, boolean isRequired) {
        m_name = name;
        m_description = description;
        m_isRequired = isRequired;
    }
    
    public void setName(String name) {
        m_name = name;
    }
    
    public void setDescription(String description) {
        m_description = description;
    }
    
    public void setRequired(boolean isRequired) {
        m_isRequired = isRequired;
    }
    
    public String name() {
        return m_name;
    }
    
    public String description() {
        return m_description;
    }

    public boolean isRequired() {
        return m_isRequired;
    }

    public String toString() {
        return String.format("%s, %s, %b", m_name, m_description, m_isRequired);
    }
}
