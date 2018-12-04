package fi.hsl.transitdata.tripupdate.processing;

import fi.hsl.common.transitdata.TransitdataProperties;

import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class ProcessorUtils {

    static final String JORE_ROUTE_NAME_REGEX = "^\\d{4}([a-zA-Z]{1}[a-zA-Z0-9]{0,1}$|[a-zA-Z ]{1}\\d{1}$|$)";
    static final Pattern JORE_ROUTE_PATTERN = Pattern.compile(JORE_ROUTE_NAME_REGEX);

    // Currently route IDs for trains are 3001 and 3002.
    static final String TRAIN_ROUTE_NAME_REGEX = "^300(1|2)";
    static final Pattern TRAIN_ROUTE_PATTERN = Pattern.compile(TRAIN_ROUTE_NAME_REGEX);


    static boolean validateRouteName(String routeName) {
        Matcher matcher = JORE_ROUTE_PATTERN.matcher(routeName);
        return matcher.matches();
    }

    static boolean isTrainRoute(String routeName) {
        Matcher matcher = TRAIN_ROUTE_PATTERN.matcher(routeName);
        return matcher.find();
    }
}
