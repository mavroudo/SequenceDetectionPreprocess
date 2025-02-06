package auth.datalab.siesta.S3ConnectorStreaming;

import auth.datalab.siesta.BusinessLogic.Model.EventStream;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.kafka.common.serialization.Deserializer;

import java.io.IOException;
import java.util.Map;



public class EventDeserializer implements Deserializer<EventStream> {

    private ObjectMapper objectMapper = new ObjectMapper();

    @Override
    public void configure(Map<String, ?> configs, boolean isKey) {

    }

    @Override
    public EventStream deserialize(String topic, byte[] data) {
        if (data == null) {
            return null;
        }
        try {
            // Assuming the EventStream class has a suitable constructor or a builder
            return objectMapper.readValue(data, EventStream.class);
        } catch (IOException e) {
            throw new RuntimeException("Failed to deserialize EventStream", e);
        }
//        JSONObject x = new JSONObject(new String(data));
//        return new EventStream(x.getLong("trace"),x.getString("event_type"), Timestamp.valueOf(x.getString("timestamp")));
    }

    @Override
    public void close() {

    }
}
