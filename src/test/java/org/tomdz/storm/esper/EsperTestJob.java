package org.tomdz.storm.esper;

import org.apache.storm.Config;
import org.apache.storm.testing.CompleteTopologyParam;
import org.apache.storm.testing.MockedSources;
import org.apache.storm.testing.TestJob;
import org.apache.storm.tuple.Tuple;
import org.apache.storm.tuple.Values;

import java.util.HashSet;

import static org.testng.Assert.assertEquals;

public abstract class EsperTestJob implements TestJob {

    /**
     * Check that all outgoing fields from the Esper bolt match the incoming fields of the
     * gathering bolt.
     * @param esperBolt
     * @param gatheringBolt
     */
    public void assertEventTypesEqual(EsperBolt esperBolt, GatheringBolt gatheringBolt) {
        for (Tuple tuple : gatheringBolt.getGatheredData()) {
            String streamId = tuple.getSourceStreamId();

            EventTypeDescriptor eventType = esperBolt.getEventTypeForStreamId(streamId);

            assertEquals(new HashSet<String>(tuple.getFields().toList()),
                new HashSet<String>(eventType.getFields().toList()));
        }
    }

    public CompleteTopologyParam createTestDataConfig(String spoutName, Values... values) {
        MockedSources mockedSources = new MockedSources();
        mockedSources.addMockData(spoutName, values);

        return createConfig(mockedSources);
    }

    public CompleteTopologyParam createConfig(MockedSources mockedSources) {
        Config conf = new Config();
        conf.setNumWorkers(1);
        conf.setDebug(false);
        conf.put(Config.TOPOLOGY_DEBUG, false);
        conf.put(Config.TOPOLOGY_EVENTLOGGER_EXECUTORS, 0);

        CompleteTopologyParam completeTopologyParam = new CompleteTopologyParam();
        completeTopologyParam.setMockedSources(mockedSources);
        completeTopologyParam.setStormConf(conf);
        return completeTopologyParam;
    }
}
