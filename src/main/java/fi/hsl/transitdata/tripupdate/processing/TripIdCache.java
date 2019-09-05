package fi.hsl.transitdata.tripupdate.processing;

import com.google.common.cache.CacheBuilder;
import com.google.common.cache.CacheLoader;
import com.google.common.cache.LoadingCache;
import fi.hsl.common.transitdata.PubtransFactory;
import org.json.JSONObject;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.*;
import java.net.URL;
import java.net.URLConnection;
import java.nio.charset.StandardCharsets;
import java.util.Objects;
import java.util.Optional;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;

/**
 * Google needs trip ID for cancellations.
 * As Transitdata pipeline does not handle trip ids, we fetch and cache trip ID from Digitransit API to use for cancellations.
 */
public class TripIdCache {
    private static final Logger logger = LoggerFactory.getLogger(TripIdCache.class);

    private static final String FUZZY_TRIP_QUERY = "{ fuzzyTrip(route: \"HSL:%s\", direction: %d, date: \"%s\", time: %d) { gtfsId } }";

    private LoadingCache<TripDetails, Optional<String>> tripIdCache = CacheBuilder.newBuilder().expireAfterAccess(1, TimeUnit.HOURS).build(new CacheLoader<TripDetails, Optional<String>>() {
        @Override
        public Optional<String> load(TripDetails tripDetails) throws Exception {
            Optional<String> maybeTripId = getTripIdFromDigitransitAPI(tripDetails.routeId, tripDetails.operatingDay, tripDetails.startTime, tripDetails.directionId);
            if (!maybeTripId.isPresent()) {
                logger.warn("No trip ID found for {} / {} / {} / {}", tripDetails.routeId, tripDetails.operatingDay, tripDetails.startTime, tripDetails.directionId);
            }
            return maybeTripId;
        }
    });

    private String digitransitApiUrl;

    public TripIdCache(String digitransitApiUrl) {
        this.digitransitApiUrl = digitransitApiUrl;
    }

    public Optional<String> getTripId(String routeId, String operatingDay, String startTime, int directionId) {
        try {
            return tripIdCache.get(new TripDetails(routeId, operatingDay, startTime, directionId));
        } catch (ExecutionException e) {
            logger.warn("Failed to fetch trip id for {} / {} / {} / {}", routeId, operatingDay, startTime, directionId, e);
            return Optional.empty();
        }
    }

    private Optional<String> getTripIdFromDigitransitAPI(String routeId, String operatingDay, String startTime, int directionId) throws IOException {
        URLConnection connection = new URL(digitransitApiUrl).openConnection();
        connection.setDoOutput(true);
        connection.setRequestProperty("Content-Type", "application/graphql");
        connection.connect();

        try (OutputStream os = connection.getOutputStream()) {
            os.write(String.format(FUZZY_TRIP_QUERY, routeId, PubtransFactory.joreDirectionToGtfsDirection(directionId), operatingDay, startTimeToSeconds(startTime)).getBytes(StandardCharsets.UTF_8));
        }

        try (InputStream is = connection.getInputStream()) {
            StringBuilder response = new StringBuilder();
            BufferedReader reader = new BufferedReader(new InputStreamReader(is));

            String line;
            while ((line = reader.readLine()) != null) {
                response.append(line);
            }

            JSONObject jsonResponse = new JSONObject(response.toString());
            if (jsonResponse.has("data")) {
                JSONObject data = jsonResponse.optJSONObject("data");
                if (data.has("fuzzyTrip")) {
                    JSONObject fuzzyTrip = data.optJSONObject("fuzzyTrip");
                    if (fuzzyTrip.has("gtfsId")) {
                        String tripId = fuzzyTrip.optString("gtfsId");
                        if (tripId != null) {
                            return Optional.of(tripId.replaceFirst("HSL:", ""));
                        }
                    }
                }
            }

            return Optional.empty();
        }
    }

    private static int startTimeToSeconds(String startTime) {
        String[] startTimeSplit = startTime.split(":");
        if (startTimeSplit.length != 3) {
            throw new IllegalArgumentException();
        }
        try {
            return Integer.parseInt(startTimeSplit[0]) * 60 * 60 + Integer.parseInt(startTimeSplit[1]) * 60 + Integer.parseInt(startTimeSplit[2]);
        } catch (NumberFormatException e) {
            throw new IllegalArgumentException();
        }
    }

    private static class TripDetails {
        final String routeId;
        final String operatingDay;
        final String startTime;
        final int directionId;

        TripDetails(String routeId, String operatingDay, String startTime, int directionId) {
            this.routeId = routeId;
            this.operatingDay = operatingDay;
            this.startTime = startTime;
            this.directionId = directionId;
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) return true;
            if (o == null || getClass() != o.getClass()) return false;
            TripDetails that = (TripDetails) o;
            return directionId == that.directionId &&
                    Objects.equals(routeId, that.routeId) &&
                    Objects.equals(operatingDay, that.operatingDay) &&
                    Objects.equals(startTime, that.startTime);
        }

        @Override
        public int hashCode() {
            return Objects.hash(routeId, operatingDay, startTime, directionId);
        }
    }
}
