package fi.hsl.transitdata.tripupdate;

import fi.hsl.common.transitdata.TransitdataProperties;
import fi.hsl.common.transitdata.proto.PubtransTableProtos;

import java.util.HashMap;
import java.util.Map;

public class MockDataFactory {


    public static StopEvent mockStopEvent(PubtransTableProtos.Common common, Map<String, String> mockProps, StopEvent.EventType eventType) {
        return StopEvent.newInstance(common, mockProps, eventType);
    }

    public static Map<String, String> mockMessageProperties(long stopId, int direction, String routeName, String operatingDay, String startTime) {

        Map<String, String> props = new HashMap<>();
        props.put(TransitdataProperties.KEY_DIRECTION, Integer.toString(direction));
        props.put(TransitdataProperties.KEY_ROUTE_NAME, routeName);
        props.put(TransitdataProperties.KEY_OPERATING_DAY, operatingDay);
        props.put(TransitdataProperties.KEY_START_TIME, startTime);
        props.put(TransitdataProperties.KEY_STOP_ID, Long.toString(stopId));
        return props;
    }

    public static PubtransTableProtos.Common mockCommon(long dvjId, int stopSequence, long jppId) {
        PubtransTableProtos.Common.Builder commonBuilder = PubtransTableProtos.Common.newBuilder();
        commonBuilder.setIsOnDatedVehicleJourneyId(dvjId);
        commonBuilder.setIsTargetedAtJourneyPatternPointGid(jppId);
        commonBuilder.setJourneyPatternSequenceNumber(stopSequence);

        commonBuilder.setState(3L);
        commonBuilder.setTargetDateTime("2018-12-24 18:00:00");
        commonBuilder.setSchemaVersion(commonBuilder.getSchemaVersion());

        commonBuilder.setId(987654321L);
        commonBuilder.setIsTimetabledAtJourneyPatternPointGid(1);
        commonBuilder.setVisitCountNumber(2);
        commonBuilder.setType(3);
        commonBuilder.setIsValidYesNo(true);
        commonBuilder.setLastModifiedUtcDateTime(1536218315L);

        return commonBuilder.build();
    }
}
