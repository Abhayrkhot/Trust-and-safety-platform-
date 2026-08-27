package dev.trustsafety.serde;

import com.fasterxml.jackson.databind.DeserializationFeature;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import dev.trustsafety.model.SafetyEvent;
import java.io.IOException;
import java.time.Instant;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;

public final class SafetyEventJson {
  private static final ObjectMapper MAPPER = new ObjectMapper()
      .enable(DeserializationFeature.FAIL_ON_TRAILING_TOKENS);
  private SafetyEventJson() {}

  public static SafetyEvent decode(byte[] payload) throws IOException {
    JsonNode n = MAPPER.readTree(payload);
    if (n == null || !n.isObject()) throw new IOException("event must be a JSON object");
    int version=requiredInt(n,"schema_version");if(version!=1&&version!=2)throw new IOException("unsupported schema_version: "+version);rejectUnknown(n,version);
    try {
      String eventId=requiredText(n,"event_id");
      return new SafetyEvent(version, eventId,
          Instant.parse(requiredText(n, "occurred_at")), Instant.parse(requiredText(n, "ingested_at")),
          version==1?"default":requiredText(n,"tenant_id"),version==1?eventId:optionalText(n,"trace_id"),
          requiredText(n, "actor_id"), optionalText(n, "content_id"),
          SafetyEvent.EventType.valueOf(requiredText(n, "event_type")), requiredInt(n, "severity"),
          attributes(n.get("attributes")));
    } catch (RuntimeException e) { throw new IOException("invalid safety event: " + e.getMessage(), e); }
  }

  private static void rejectUnknown(JsonNode n,int version) throws IOException {
    var allowed = version==1
        ?java.util.Set.of("schema_version","event_id","occurred_at","ingested_at","actor_id","content_id","event_type","severity","attributes")
        :java.util.Set.of("schema_version","event_id","occurred_at","ingested_at","tenant_id","trace_id","actor_id","content_id","event_type","severity","attributes");
    Iterator<String> fields = n.fieldNames();
    while (fields.hasNext()) { String f = fields.next(); if (!allowed.contains(f)) throw new IOException("unknown field: " + f); }
  }
  private static String requiredText(JsonNode n, String f) { JsonNode v=n.get(f); if(v==null||!v.isTextual()||v.textValue().isBlank()) throw new IllegalArgumentException(f+" must be text"); return v.textValue(); }
  private static String optionalText(JsonNode n, String f) { JsonNode v=n.get(f); return v==null||v.isNull()?null:requiredText(n,f); }
  private static int requiredInt(JsonNode n, String f) { JsonNode v=n.get(f); if(v==null||!v.canConvertToInt()) throw new IllegalArgumentException(f+" must be integer"); return v.intValue(); }
  private static Map<String,String> attributes(JsonNode n) { if(n==null||n.isNull()) return Map.of(); if(!n.isObject()) throw new IllegalArgumentException("attributes must be object"); Map<String,String> out=new HashMap<>(); n.fields().forEachRemaining(e->{if(!e.getValue().isTextual()) throw new IllegalArgumentException("attribute values must be strings"); out.put(e.getKey(),e.getValue().textValue());}); return out; }
}
