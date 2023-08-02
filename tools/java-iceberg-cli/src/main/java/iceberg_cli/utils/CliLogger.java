package iceberg_cli.utils;

import org.apache.log4j.BasicConfigurator;
import org.apache.log4j.ConsoleAppender;
import org.apache.log4j.EnhancedPatternLayout;
import org.apache.log4j.Level;
import org.apache.log4j.Logger;

public class CliLogger {

    private static Logger m_logger;

    public static Logger getLogger() {
        return m_logger;
    }

    public static void setupLogging() {
        /* Initialize and then disable logging from the root logger. This is because
         * many 3rd party packages want to log output to the root logger which will
         * spam our logfiles with unwanted information.
         */
        BasicConfigurator.configure();
        Logger.getRootLogger().setLevel(Level.OFF);

        /* Now setup our own logger which matches our typical logfile format. Since we
         * redirect stderr output to the log currently we'll setup a ConsoleAppender.
         * We can consider removing this and replacing with a file logger in the future.
         */
        EnhancedPatternLayout layout = new EnhancedPatternLayout();
        layout.setConversionPattern("%d{yyyy-MM-dd HH:mm:ss.SSSSSS zzz} %p: %m%n");
        ConsoleAppender ap = new ConsoleAppender(layout);
        ap.setTarget("System.err");
        m_logger = Logger.getLogger("java-iceberg-toolkit");
        m_logger.setAdditivity(false);
        m_logger.removeAllAppenders();
        m_logger.addAppender(ap);
        m_logger.setLevel(Level.INFO);
    }
}
