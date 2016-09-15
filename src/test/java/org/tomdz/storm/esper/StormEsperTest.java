package org.tomdz.storm.esper;

import org.apache.storm.Config;
import org.apache.storm.ILocalCluster;
import org.apache.storm.Testing;
import org.apache.storm.generated.StormTopology;
import org.apache.storm.testing.CompleteTopologyParam;
import org.apache.storm.testing.MkClusterParam;
import org.apache.storm.testing.MockedSources;
import org.apache.storm.testing.TestJob;
import org.apache.storm.topology.TopologyBuilder;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Values;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import java.util.Map;

import static org.testng.Assert.assertTrue;

@Test
public class StormEsperTest
{
    private final String GATHERING_BOLT_ID = "gathering-bolt";

    @BeforeMethod(alwaysRun = true)
    public void setUp()
    {
    }

    @AfterMethod(alwaysRun = true)
    public void tearDown() throws Exception
    {
    }

    private MkClusterParam getMkClusterParam(){
        MkClusterParam mkClusterParam = new MkClusterParam();
        mkClusterParam.setSupervisors(1);
        Config daemonConf = new Config();
        daemonConf.put(Config.STORM_LOCAL_MODE_ZMQ, false);
        mkClusterParam.setDaemonConf(daemonConf);
        return mkClusterParam;
    }

    @Test
    public void testSimple() throws Exception
    {
        TestJob testJob = new EsperTestJob(
        ) {
            @Override
            public void run(ILocalCluster cluster) {
                String spoutId = "spout";
                String esperBoltId = "esper-bolt";
                String eventId = "TestEvent";

                // build the test topology
                TopologyBuilder builder = new TopologyBuilder();
                TestSpout spout = new TestSpout(new Fields("a", "b"));
                builder.setSpout(spoutId, spout);

                EsperBolt esperBolt = new EsperBolt.Builder()
                    .inputs().aliasComponent(spoutId).withFields("a", "b").ofType(Integer.class).toEventType(eventId)
                    .outputs().onDefaultStream().emit("max", "sum")
                    .statements().add("select max(a) as max, sum(b) as sum from " + eventId + ".win:length_batch(4)")
                    .build();
                builder
                    .setBolt(esperBoltId, esperBolt)
                    .globalGrouping(spoutId);

                GatheringBolt gatheringBolt = new GatheringBolt();
                builder.setBolt(GATHERING_BOLT_ID, gatheringBolt).globalGrouping(esperBoltId);

                StormTopology topology = builder.createTopology();

                CompleteTopologyParam completeTopologyParam = createTestDataConfig(
                    spoutId, new Values(4, 1), new Values(2, 3), new Values(1, 2), new Values(3, 4)
                );

                Map result = Testing.completeTopology(cluster, topology, completeTopologyParam);
                assertTrue(
                    Testing.multiseteq(
                        new Values(
                            new Values(4, 10)
                        ),
                        Testing.readTuples(result, esperBoltId)
                    )
                );
                assertEventTypesEqual(esperBolt, gatheringBolt);
            }
        };
        Testing.withSimulatedTimeLocalCluster(getMkClusterParam(), testJob);
    }

    @Test
    public void testMultipleStatements() throws Exception
    {
        TestJob testJob = new EsperTestJob(
        ) {
            @Override
            public void run(ILocalCluster cluster) {
                String spoutId = "spout";
                String esperBoltId = "esper-bolt";
                String stream1Id = "stream1";
                String stream2Id = "stream2";
                String eventId = "TestEvent";

                // build the test topology
                TopologyBuilder builder = new TopologyBuilder();
                TestSpout spout = new TestSpout(new Fields("a", "b"));
                builder.setSpout(spoutId, spout);

                EsperBolt esperBolt = new EsperBolt.Builder()
                    .inputs().aliasComponent(spoutId).toEventType(eventId)
                    .outputs().onStream(stream1Id).fromEventType("MaxValue").emit("max")
                    .onStream(stream2Id).fromEventType("MinValue").emit("min")
                    .statements().add("insert into MaxValue select max(a) as max from " + eventId + ".win:length_batch(4)")
                    .add("insert into MinValue select min(b) as min from " + eventId + ".win:length_batch(4)")
                    .build();
                builder
                    .setBolt(esperBoltId, esperBolt)
                    .globalGrouping(spoutId);

                GatheringBolt gatheringBolt = new GatheringBolt();
                builder.setBolt(GATHERING_BOLT_ID, gatheringBolt)
                    .globalGrouping(esperBoltId, stream1Id)
                    .globalGrouping(esperBoltId, stream2Id);

                StormTopology topology = builder.createTopology();

                CompleteTopologyParam completeTopologyParam = createTestDataConfig(
                    spoutId, new Values(4, 1), new Values(2, 3), new Values(1, 2), new Values(3, 4)
                );

                Map result = Testing.completeTopology(cluster, topology, completeTopologyParam);
                assertTrue(
                    Testing.multiseteq(
                        new Values(
                            new Values(4)
                        ),
                        Testing.readTuples(result, esperBoltId, stream1Id)
                    )
                );
                assertTrue(
                    Testing.multiseteq(
                        new Values(
                            new Values(1)
                        ),
                        Testing.readTuples(result, esperBoltId, stream2Id)
                    )
                );
                assertEventTypesEqual(esperBolt, gatheringBolt);
            }
        };
        Testing.withSimulatedTimeLocalCluster(getMkClusterParam(), testJob);
    }

    @Test
    public void testMultipleSpouts() throws Exception
    {
        TestJob testJob = new EsperTestJob(
        ) {
            @Override
            public void run(ILocalCluster cluster) {
                String spout1Id = "spout1";
                String spout2Id = "spout2";
                String event1Id = "Event1";
                String event2Id = "Event2";
                String esperBoltId = "esper";

                // build the test topology
                TopologyBuilder builder = new TopologyBuilder();
                TestSpout spout1 = new TestSpout(new Fields("a"));
                builder.setSpout(spout1Id, spout1);
                TestSpout spout2 = new TestSpout(new Fields("b"));
                builder.setSpout(spout2Id, spout2);

                EsperBolt esperBolt = new EsperBolt.Builder()
                                           .inputs().aliasComponent(spout1Id).toEventType(event1Id)
                                                    .aliasComponent(spout2Id).toEventType(event2Id)
                                           .outputs().onDefaultStream().emit("min", "max")
                                           .statements().add(
                                               "select max(a) as max, min(b) as min " +
                                               "from " + event1Id + ".win:length_batch(4), " + event2Id + ".win:length_batch(4)"
                                            )
                                           .build();
                builder
                    .setBolt(esperBoltId, esperBolt)
                    .globalGrouping(spout1Id)
                    .globalGrouping(spout2Id);

                GatheringBolt gatheringBolt = new GatheringBolt();
                builder.setBolt(GATHERING_BOLT_ID, gatheringBolt).globalGrouping(esperBoltId);

                StormTopology topology = builder.createTopology();

                MockedSources mockedSources = new MockedSources();
                mockedSources.addMockData(spout1Id, new Values(4), new Values(2), new Values(1), new Values(3));
                mockedSources.addMockData(spout2Id, new Values(1), new Values(3), new Values(2), new Values(4));

                CompleteTopologyParam completeTopologyParam = createConfig(mockedSources);

                Map result = Testing.completeTopology(cluster, topology, completeTopologyParam);
                assertTrue(
                    Testing.multiseteq(
                        new Values(
                            new Values(1, 4)
                        ),
                        Testing.readTuples(result, esperBoltId)
                    )
                );
                assertEventTypesEqual(esperBolt, gatheringBolt);
            }
        };
        Testing.withSimulatedTimeLocalCluster(getMkClusterParam(), testJob);
    }

    @Test
    public void testNoInputAlias() throws Exception
    {
        TestJob testJob = new EsperTestJob(
        ) {
            @Override
            public void run(ILocalCluster cluster) {
                String spoutId = "spout";
                String esperBoltId = "esper-bolt";
                // build the test topology
                TopologyBuilder builder = new TopologyBuilder();
                TestSpout spout = new TestSpout(new Fields("a", "b"));
                builder.setSpout(spoutId, spout);

                EsperBolt esperBolt = new EsperBolt.Builder()
                    .outputs().onDefaultStream().emit("min", "max")
                    .statements().add("select max(a) as max, min(b) as min from " + spoutId + "_default.win:length_batch(4)")
                    .build();
                builder
                    .setBolt(esperBoltId, esperBolt)
                    .globalGrouping(spoutId);

                GatheringBolt gatheringBolt = new GatheringBolt();
                builder.setBolt(GATHERING_BOLT_ID, gatheringBolt)
                    .globalGrouping(esperBoltId);

                StormTopology topology = builder.createTopology();

                CompleteTopologyParam completeTopologyParam = createTestDataConfig(
                    spoutId, new Values(4, 1), new Values(2, 3), new Values(1, 2), new Values(3, 4)
                );

                Map result = Testing.completeTopology(cluster, topology, completeTopologyParam);
                assertTrue(
                    Testing.multiseteq(
                        new Values(
                            new Values(1, 4)
                        ),
                        Testing.readTuples(result, esperBoltId, "default")
                    )
                );
                assertEventTypesEqual(esperBolt, gatheringBolt);
            }
        };
        Testing.withSimulatedTimeLocalCluster(getMkClusterParam(), testJob);
    }

    @Test
    public void testMultipleSpoutsWithoutInputAlias() throws Exception
    {
        TestJob testJob = new EsperTestJob(
        ) {
            @Override
            public void run(ILocalCluster cluster) {
                String spout1Id = "spout1";
                String spout2Id = "spout2";
                String esperBoltId = "esper-bolt";
                String eventId = "TestEvent";

                // build the test topology
                TopologyBuilder builder = new TopologyBuilder();
                TestSpout spout1 = new TestSpout(new Fields("a"));
                builder.setSpout(spout1Id, spout1);
                TestSpout spout2 = new TestSpout(new Fields("b"));
                builder.setSpout(spout2Id, spout2);

                EsperBolt esperBolt = new EsperBolt.Builder()
                    .inputs().aliasStream(spout1Id, "default").toEventType(eventId)
                    .outputs().onDefaultStream().emit("min", "max")
                    .statements().add("select max(a) as max, min(b) as min from " + eventId + ".win:length_batch(4), " + spout2Id + "_default.win:length_batch(4)")
                    .build();
                
                builder
                    .setBolt(esperBoltId, esperBolt)
                    .globalGrouping(spout1Id)
                    .globalGrouping(spout2Id);

                GatheringBolt gatheringBolt = new GatheringBolt();
                builder.setBolt(GATHERING_BOLT_ID, gatheringBolt)
                    .globalGrouping(esperBoltId);

                StormTopology topology = builder.createTopology();

                MockedSources mockedSources = new MockedSources();
                mockedSources.addMockData(spout1Id, new Values(4), new Values(2), new Values(1), new Values(3));
                mockedSources.addMockData(spout2Id, new Values(1), new Values(3), new Values(2), new Values(4));
                CompleteTopologyParam completeTopologyParam = createConfig(mockedSources);

                Map result = Testing.completeTopology(cluster, topology, completeTopologyParam);
                assertTrue(
                    Testing.multiseteq(
                        new Values(
                            new Values(1, 4)
                        ),
                        Testing.readTuples(result, esperBoltId, "default")
                    )
                );
                assertEventTypesEqual(esperBolt, gatheringBolt);
            }
        };
        Testing.withSimulatedTimeLocalCluster(getMkClusterParam(), testJob);
    }

    @Test
    public void testMultipleBolts() throws Exception
    {
        TestJob testJob = new EsperTestJob(
        ) {
            @Override
            public void run(ILocalCluster cluster) {
                String spout1Id = "spout1";
                String spout2Id = "spout2";
                String esperBolt1Id = "esper-bolt1";
                String esperBolt2Id = "esper-bolt2";
                String event1Id = "TestEvent1";
                String event2Id = "TestEvent2";

                // build the test topology
                TopologyBuilder builder = new TopologyBuilder();
                TestSpout spout1 = new TestSpout(new Fields("a"));
                builder.setSpout(spout1Id, spout1);
                TestSpout spout2 = new TestSpout(new Fields("b"));
                builder.setSpout(spout2Id, spout2);

                EsperBolt esperBolt1 = new EsperBolt.Builder()
                    .inputs().aliasComponent(spout1Id).toEventType(event1Id)
                    .outputs().onDefaultStream().emit("max")
                    .statements().add("select max(a) as max from " + event1Id + ".win:length_batch(4)")
                    .build();
                EsperBolt esperBolt2 = new EsperBolt.Builder()
                    .inputs().aliasComponent(spout2Id).toEventType(event2Id)
                    .outputs().onDefaultStream().emit("min")
                    .statements().add("select min(b) as min from " + event2Id + ".win:length_batch(4)")
                    .build();

                builder
                    .setBolt(esperBolt1Id, esperBolt1)
                    .globalGrouping(spout1Id);
                builder
                    .setBolt(esperBolt2Id, esperBolt2)
                    .globalGrouping(spout2Id);

                GatheringBolt gatheringBolt = new GatheringBolt();
                builder.setBolt(GATHERING_BOLT_ID, gatheringBolt)
                    .globalGrouping(esperBolt1Id)
                    .globalGrouping(esperBolt2Id);

                StormTopology topology = builder.createTopology();

                MockedSources mockedSources = new MockedSources();
                mockedSources.addMockData(spout1Id, new Values(4), new Values(2), new Values(1), new Values(3));
                mockedSources.addMockData(spout2Id, new Values(1), new Values(3), new Values(2), new Values(4));
                CompleteTopologyParam completeTopologyParam = createConfig(mockedSources);

                Map result = Testing.completeTopology(cluster, topology, completeTopologyParam);
                assertTrue(
                    Testing.multiseteq(
                        new Values(
                            new Values(4)
                        ),
                        Testing.readTuples(result, esperBolt1Id, "default")
                    )
                );
                assertTrue(
                    Testing.multiseteq(
                        new Values(
                            new Values(1)
                        ),
                        Testing.readTuples(result, esperBolt2Id, "default")
                    )
                );
            }
        };
        Testing.withSimulatedTimeLocalCluster(getMkClusterParam(), testJob);
    }

    // TODO: more tests
    // adding aliasComponent for undefined spout
    // using the same aliasComponent twice
    // using same stream id twice for named output
    // multiple esper bolts
}
