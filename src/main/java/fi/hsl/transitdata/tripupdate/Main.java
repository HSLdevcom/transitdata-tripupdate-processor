package fi.hsl.transitdata.tripupdate;

import com.typesafe.config.Config;
import fi.hsl.common.ConfigParser;
import fi.hsl.common.pulsar.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class Main {
    private static final Logger log = LoggerFactory.getLogger(Main.class);

    public static void main(String[] args) {
        Config config = ConfigParser.createConfig();
        try (PulsarApplication app = PulsarApplication.newInstance(config)) {

            PulsarApplicationContext context = app.getContext();

            MessageRouter router = new MessageRouter(context);

            app.launchWithHandler(router);
        } catch (Exception e) {
            log.error("Exception at main", e);
        }
    }
}
