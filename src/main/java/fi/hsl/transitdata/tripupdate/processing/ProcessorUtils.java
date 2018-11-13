package fi.hsl.transitdata.tripupdate.processing;

import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class ProcessorUtils {

    static final String JORE_ROUTE_NAME_REGEX = "^\\d{4}([a-zA-Z]{1}[a-zA-Z0-9]{0,1}$|[a-zA-Z ]{1}\\d{1}$|$)";

    static boolean validateRouteName(String routeName) {

        Pattern routePattern = Pattern.compile(JORE_ROUTE_NAME_REGEX);
        Matcher matcher = routePattern.matcher(routeName);

        return matcher.matches();
    }
}
