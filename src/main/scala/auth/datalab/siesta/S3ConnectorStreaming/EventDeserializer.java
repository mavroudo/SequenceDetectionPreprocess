package auth.datalab.siesta.S3ConnectorStreaming;

import auth.datalab.siesta.BusinessLogic.Model.Structs;
import com.datastax.oss.driver.shaded.json.JSONObject;
import org.apache.kafka.common.serialization.Deserializer;

import java.sql.Timestamp;
import java.util.Arrays;
import java.util.Map;

public class EventDeserializer implements Deserializer<Structs.EventStream> {
    @Override
    public void configure(Map<String, ?> configs, boolean isKey) {

    }

    @Override
    public Structs.EventStream deserialize(String topic, byte[] data) {
        JSONObject x = new JSONObject(new String(data));
        return new Structs.EventStream(x.getLong("trace"),x.getString("event_type"), Timestamp.valueOf(x.getString("timestamp")));
    }

    @Override
    public void close() {

    }
}
