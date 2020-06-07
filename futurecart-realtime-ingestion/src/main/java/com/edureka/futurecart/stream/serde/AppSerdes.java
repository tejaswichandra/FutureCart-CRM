package com.edureka.futurecart.stream.serde;

import com.edureka.futurecart.stream.types.SurveyRecord;
import com.edureka.futurecart.stream.types.CaseRecord;
import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.serialization.Serdes;

import java.util.HashMap;
import java.util.Map;



public class AppSerdes extends Serdes {

    static final class SurveyRecordSerde extends Serdes.WrapperSerde<SurveyRecord> {
        SurveyRecordSerde() {
            super(new JsonSerializer<>(), new JsonDeserializer<>());
        }
    }

    public static Serde<SurveyRecord> SurveyRecord() {
        SurveyRecordSerde serde = new SurveyRecordSerde();

        Map<String, Object> serdeConfigs = new HashMap<>();
        serdeConfigs.put(JsonDeserializer.VALUE_CLASS_NAME_CONFIG, SurveyRecord.class);
        serde.configure(serdeConfigs, false);

        return serde;
    }

    static final class CaseRecordSerde extends Serdes.WrapperSerde<CaseRecord> {
        CaseRecordSerde() {
            super(new JsonSerializer<>(), new JsonDeserializer<>());
        }
    }
    public static Serde<CaseRecord> CaseRecord() {
        CaseRecordSerde serde = new CaseRecordSerde();

        Map<String, Object> serdeConfigs = new HashMap<>();
        serdeConfigs.put(JsonDeserializer.VALUE_CLASS_NAME_CONFIG, CaseRecord.class);
        serde.configure(serdeConfigs, false);

        return serde;
    }

}
