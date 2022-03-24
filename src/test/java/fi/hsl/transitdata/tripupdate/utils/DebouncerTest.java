package fi.hsl.transitdata.tripupdate.utils;

import org.junit.Test;

import java.time.Duration;
import java.util.concurrent.atomic.AtomicInteger;

import static org.junit.Assert.assertEquals;

public class DebouncerTest {
    @Test
    public void testDebouncer() throws InterruptedException {
        Debouncer debouncer = new Debouncer(Duration.ofMillis(500));

        AtomicInteger count = new AtomicInteger();

        for (int i = 0; i < 500; i++) {
            debouncer.debounce("x", count::getAndIncrement);
        }
        Thread.sleep(600);

        assertEquals(1, count.get());

        for (int i = 0; i < 500; i++) {
            debouncer.debounce("x", count::getAndIncrement);
        }
        Thread.sleep(600);

        assertEquals(2, count.get());
    }
}
