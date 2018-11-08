package fi.hsl.transitdata.tripupdate.processing;

import fi.hsl.common.transitdata.TransitdataProperties;
import fi.hsl.transitdata.tripupdate.MockDataFactory;
import org.junit.Test;

import java.util.HashMap;
import java.util.Map;

import static org.junit.Assert.assertEquals;

public class BaseProcessorTest {

    @Test
    public void messageWithEmptyPropertiesIsDiscarded() {

        Map<String, String> properties = new HashMap<>();

        assertEquals(false, BaseProcessor.validateRequiredProperties(properties));
    }

    @Test
    public void messageWithOneMissingPropertyIsDiscarded() {

        Map<String, String> properties = new HashMap<>();
        properties.put(TransitdataProperties.KEY_ROUTE_NAME, "1014");
        properties.put(TransitdataProperties.KEY_DIRECTION, "0");
        properties.put(TransitdataProperties.KEY_OPERATING_DAY, "20181109");
        //No start time

        assertEquals(false, BaseProcessor.validateRequiredProperties(properties));
    }

    @Test
    public void messageWithRequiredPropertiesIsAccepted() {

        Map<String, String> properties = new HashMap<>();
        properties.put(TransitdataProperties.KEY_ROUTE_NAME, "1014");
        properties.put(TransitdataProperties.KEY_DIRECTION, "0");
        properties.put(TransitdataProperties.KEY_OPERATING_DAY, "20181109");
        properties.put(TransitdataProperties.KEY_START_TIME, "11:22:00");

        assertEquals(true, BaseProcessor.validateRequiredProperties(properties));

    }

    @Test
    public void messageForTrainRouteKIsDiscarded() {

        Map<String, String> properties = new HashMap<>();
        properties.put(TransitdataProperties.KEY_ROUTE_NAME, "3001K");
        properties.put(TransitdataProperties.KEY_DIRECTION, "0");
        properties.put(TransitdataProperties.KEY_OPERATING_DAY, "20181109");
        properties.put(TransitdataProperties.KEY_START_TIME, "11:22:00");

        assertEquals(false, BaseProcessor.validateRequiredProperties(properties));
    }

    @Test
    public void messageForTrainRouteUIsDiscarded() {

        Map<String, String> properties = new HashMap<>();
        properties.put(TransitdataProperties.KEY_ROUTE_NAME, "3002U");
        properties.put(TransitdataProperties.KEY_DIRECTION, "0");
        properties.put(TransitdataProperties.KEY_OPERATING_DAY, "20181109");
        properties.put(TransitdataProperties.KEY_START_TIME, "11:22:00");

        assertEquals(false, BaseProcessor.validateRequiredProperties(properties));
    }
}
