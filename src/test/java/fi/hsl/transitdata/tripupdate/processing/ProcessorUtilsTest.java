package fi.hsl.transitdata.tripupdate.processing;

import org.junit.Test;

import static org.junit.Assert.assertEquals;

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

}
