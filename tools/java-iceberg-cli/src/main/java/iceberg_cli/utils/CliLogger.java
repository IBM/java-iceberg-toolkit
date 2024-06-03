package iceberg_cli.utils;

import org.apache.log4j.BasicConfigurator;
import org.apache.log4j.EnhancedPatternLayout;
import org.apache.log4j.FileAppender;
import org.apache.log4j.Layout;
import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.log4j.PatternLayout;
import org.apache.log4j.spi.LoggingEvent;
import org.slf4j.LoggerFactory;

import java.io.IOException;

import static org.slf4j.Logger.ROOT_LOGGER_NAME;

public class CliLogger {

    private static Logger m_logger;

    public static Logger getLogger() throws IOException {
        if (m_logger == null) {
            try {
                setupLogging();
            } catch (IOException e) {
                System.err.println("Failed setting up logger: " + e.getMessage());
                throw e;
            }
        }
        return m_logger;
    }

    public static void setupLogging() throws IOException {
        /* Initialize and then disable logging from the root logger. This is because
         * many 3rd party packages want to log output to the root logger which will
         * spam our logfiles with unwanted information.
         */
        BasicConfigurator.configure();
        Logger.getRootLogger().setLevel(Level.OFF);
        ch.qos.logback.classic.Logger slf4jLogger = (ch.qos.logback.classic.Logger) LoggerFactory.getLogger(ROOT_LOGGER_NAME);
        slf4jLogger.setLevel(ch.qos.logback.classic.Level.OFF);

        m_logger = Logger.getLogger("java-iceberg-toolkit");
        m_logger.setAdditivity(false);
        m_logger.removeAllAppenders();
        
        /* Now setup our own logger which matches our typical log file format.
         * Try creating a file logger wit a custom appender for masking sensitive
         * information.
         */
        EnhancedPatternLayout layout = new EnhancedPatternLayout();
        layout.setConversionPattern("%d{yyyy-MM-dd HH:mm:ss.SSSSSS zzz} %p: %m%n");
        try {
            String logfile = System.getProperty("LOG_FILE");
            MaskFileAppender maskAppender = MaskFileAppender.createFileAppender(layout, logfile);
            m_logger.addAppender(maskAppender);
        } catch (IOException e) {
            String errMsg = String.format("Error setting up file logger for iceberg_cli: ", e.getMessage());
            System.err.println(errMsg);
            throw e;
        }
        
        m_logger.setLevel(Level.INFO);
    }
    
    /**
     * Custom appender that masks sensitive information when logging.
     * filename: -DLOG_FILE will be used as the log file path,
     * icebergcli_logs/iceberg_cli.log be used by default.
     * layout: EnhancedPatternLayout will be used as the layout.
     *
     */
    static class MaskFileAppender extends FileAppender {
        private MaskFileAppender(Layout layout, String filename) throws IOException {
            super(layout, filename);
        }
        
        public static MaskFileAppender createFileAppender(Layout layout, String filename) throws IOException {
            if (layout == null) {
                layout = new PatternLayout();
            }
            if (filename == null) {
                filename = "icebergcli_logs/iceberg_cli.log";
            }

            return new MaskFileAppender(layout, filename);
        }
        
        @Override
        public void append(LoggingEvent event) {
            String message = event.getMessage().toString();
                        
            Throwable throwable = event.getThrowableInformation() != null ? event.getThrowableInformation().getThrowable() : null;
            LoggingEvent maskedEvent = new LoggingEvent(event.fqnOfCategoryClass, Logger.getLogger(event.getLoggerName()),
                                                        event.timeStamp, event.getLevel(), maskMessage(message), throwable);
            super.append(maskedEvent);
        }
    }
    
    public static String maskMessage(String message) {
        String keywords = "AWS_ACCESS_KEY_ID|AWS_SECRET_ACCESS_KEY|AWS_SESSION_TOKEN|AZURE_CLIENT_ID|"
                        + "AZURE_ACCOUNT_KEY|AZURE_CLIENT_SECRET|AZURE_TENANT_ID|AZURE_SAS_TOKEN|user|"
                        + "username|password|token|api-key|secret|AccountKey|access-key|secret-key";
        
        return message.replaceAll("(--creds[, ]*)\\s*[\"']?\\{([^ ]*)\\}[\"']?", "$1'####'")
                .replaceAll("(-c[, ]*)\\s*[\"']?\\{([^ ]*)\\}[\"']?", "$1'####'")
                .replaceAll("(--conf[, ]*)\\s*[\"']?\\{([^ ]*)\\}[\"']?", "$1'####'")
                .replaceAll(String.format("([\"']?((?i)%s)[\"']?\\s*:?)\\s*[\"']?([^ ,\\}]*)[\"']?", keywords), "$1'####'");
    }
}
