package fi.hsl.transitdata.tripupdate.utils;

import java.time.Duration;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

/**
 * Helper class that allows debouncing actions
 */
public class Debouncer {
    private final ScheduledExecutorService scheduler = Executors.newSingleThreadScheduledExecutor(runnable -> {
        final Thread t = new Thread(runnable);
        t.setDaemon(true);
        return t;
    });
    private final Map<Object, Future<?>> delayedMap = new HashMap<>();

    private final Duration delay;

    /**
     *
     * @param delay Delay for debounce
     */
    public Debouncer(Duration delay) {
        this.delay = delay;
    }

    public void debounce(final Object key, final Runnable runnable) {
        final Future<?> prev = delayedMap.put(key, scheduler.schedule(() -> {
            try {
                runnable.run();
            } finally {
                delayedMap.remove(key);
            }
        }, delay.toMillis(), TimeUnit.MILLISECONDS));
        if (prev != null) {
            prev.cancel(true);
        }
    }

    public void shutdown() {
        scheduler.shutdownNow();
    }
}
