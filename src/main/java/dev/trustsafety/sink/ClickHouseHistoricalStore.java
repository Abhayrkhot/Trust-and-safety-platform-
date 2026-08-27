package dev.trustsafety.sink;

import dev.trustsafety.model.RiskSignal;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.Statement;
import java.sql.Timestamp;
import java.util.Properties;

/** Append-oriented historical store; replayed signal IDs collapse through ReplacingMergeTree. */
public final class ClickHouseHistoricalStore implements RiskSignalStore {
  private static final String TABLE="risk_signals";
  private final Connection connection; private final PreparedStatement insert;

  public ClickHouseHistoricalStore(String jdbcUrl, String username, String password) throws Exception {
    Properties properties=new Properties(); properties.setProperty("user",username); properties.setProperty("password",password);
    connection=DriverManager.getConnection(jdbcUrl,properties);
    try(Statement statement=connection.createStatement()) { statement.execute("""
        CREATE TABLE IF NOT EXISTS risk_signals (
          signal_id String, actor_id String, triggering_event_id String, rule_id LowCardinality(String),
          risk_score UInt8, reason String, observed_event_count UInt64, observed_severity_sum UInt64,
          emitted_at DateTime64(3, 'UTC'), stored_at DateTime64(3, 'UTC') DEFAULT now64(3)
        ) ENGINE = ReplacingMergeTree(stored_at) ORDER BY signal_id
        """); }
    insert=connection.prepareStatement("INSERT INTO "+TABLE+" (signal_id,actor_id,triggering_event_id,rule_id,risk_score,reason,observed_event_count,observed_severity_sum,emitted_at) VALUES (?,?,?,?,?,?,?,?,?)");
  }
  @Override public void write(RiskSignal s) throws Exception {
    insert.setString(1,s.signalId());insert.setString(2,s.actorId());insert.setString(3,s.triggeringEventId());insert.setString(4,s.ruleId());
    insert.setInt(5,s.riskScore());insert.setString(6,s.reason());insert.setLong(7,s.observedEventCount());insert.setLong(8,s.observedSeveritySum());
    insert.setTimestamp(9,Timestamp.from(s.emittedAt()));insert.executeUpdate();
  }
  @Override public void close() throws Exception { try { insert.close(); } finally { connection.close(); } }
}
