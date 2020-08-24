package fi.hsl.transitdata.tripupdate.processing;

import fi.hsl.common.transitdata.TransitdataProperties;
import org.junit.Test;

import java.util.HashMap;
import java.util.Map;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

public class ProcessorUtilsTest {

    @Test
    public void routeNameWithNoVariantsIsValid() {
        assertEquals(true, ProcessorUtils.validateRouteName("1014"));
    }

    @Test
    public void routeNameWithOneLetterIsValid() {
        assertEquals(true, ProcessorUtils.validateRouteName("6173K"));
    }

    @Test
    public void routeNameWithTwoLettersIsValid() {
        assertEquals(true, ProcessorUtils.validateRouteName("9785AK"));
    }

    @Test
    public void routeNameWithLetterAndNumberIsValid() {
        assertEquals(true, ProcessorUtils.validateRouteName("3100M2"));
    }

    @Test
    public void routeNameWithSpaceAndNumberIsValid() {
        assertEquals(true, ProcessorUtils.validateRouteName("1010 4"));
    }

    @Test
    public void routeNameWithTwoSpacesIsInvalid() {
        assertEquals(false, ProcessorUtils.validateRouteName("1010  4"));
    }

    @Test
    public void routeNameWithTrailingSpaceIsInvalid() {
        assertEquals(false, ProcessorUtils.validateRouteName("1010 "));
    }

    @Test
    public void routeNameWithTwoTrailingSpacesIsInvalid() {
        assertEquals(false, ProcessorUtils.validateRouteName("1010  "));
    }

    @Test
    public void routeNameWithLettersInBeginningIsInvalid() {
        assertEquals(false, ProcessorUtils.validateRouteName("M100"));
    }

    @Test
    public void routeNameLongerThanSixCharsIsInvalid() {
        assertEquals(false, ProcessorUtils.validateRouteName("6173AKT"));
    }

    @Test
    public void trainRouteMatches() {
        assertTrue(ProcessorUtils.isTrainRoute("3001K"));
        assertTrue(ProcessorUtils.isTrainRoute("3002U"));
        assertTrue(ProcessorUtils.isTrainRoute("3001"));
        assertTrue(ProcessorUtils.isTrainRoute("3002"));
        assertTrue(ProcessorUtils.isTrainRoute("3002 ABC"));
    }

    @Test
    public void nonTrainRoutesDoesntMatch() {
        assertFalse(ProcessorUtils.isTrainRoute("3000K"));
        assertFalse(ProcessorUtils.isTrainRoute("3003U"));
        assertFalse(ProcessorUtils.isTrainRoute("3000"));
        assertFalse(ProcessorUtils.isTrainRoute("3003"));
        assertFalse(ProcessorUtils.isTrainRoute("757"));
        assertFalse(ProcessorUtils.isTrainRoute("30002"));
    }

    @Test
    public void removeVariantWhenNoVariant(){
        assertEquals("31M1", ProcessorUtils.removeVariant("31M1"));
    }

    @Test
    public void removeVariantWhenVariant(){
        assertEquals("31M1", ProcessorUtils.removeVariant("31M1 5"));
    }

}
