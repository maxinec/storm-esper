package org.tomdz.storm.esper;

import org.apache.storm.spout.SpoutOutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.base.BaseRichSpout;
import org.apache.storm.tuple.Fields;

import java.util.Map;

public class TestSpout extends BaseRichSpout
{
    private static final long serialVersionUID = 1L;

    private final Fields fields;

    public TestSpout(Fields fields)
    {
        this.fields = fields;
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer declarer)
    {
        declarer.declare(fields);
    }

    @Override
    public void open(Map map, TopologyContext topologyContext, SpoutOutputCollector spoutOutputCollector) {

    }

    @Override
    public void nextTuple() {

    }
}
