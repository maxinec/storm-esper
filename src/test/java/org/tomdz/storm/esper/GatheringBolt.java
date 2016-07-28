package org.tomdz.storm.esper;

import org.apache.storm.task.OutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.base.BaseRichBolt;
import org.apache.storm.tuple.Tuple;
import org.apache.storm.tuple.TupleImpl;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CopyOnWriteArrayList;

public class GatheringBolt extends BaseRichBolt
{
    private static final long serialVersionUID = 1L;

    private static final List<Tuple> tuples = new CopyOnWriteArrayList<Tuple>();
    private transient TopologyContext context;
    private transient OutputCollector collector;

    @Override
    public void prepare(@SuppressWarnings("rawtypes") Map stormConf,
                        TopologyContext context,
                        OutputCollector collector)
    {
        this.context = context;
        this.collector = collector;
        tuples.clear();
    }

    @Override
    public void execute(Tuple input)
    {
        Tuple newTuple = new TupleImpl(context, input.getValues(), input.getSourceTask(), input.getSourceStreamId());

        tuples.add(newTuple);
        collector.ack(input);
    }

    public List<Tuple> getGatheredData()
    {
        return new ArrayList<Tuple>(tuples);
    }

    @Override
    public void cleanup() {}

    @Override
    public void declareOutputFields(OutputFieldsDeclarer declarer) {}
}
