package org.tomdz.storm.esper.model;

import java.util.Map;

public interface Event {
    Map<Class, String[]> getModelByType();
    String[] getModels();
    String getStreamName();
    String getEventName();
}
