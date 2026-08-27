package dev.trustsafety.sink;

import dev.trustsafety.model.RiskSignal;
import java.io.IOException;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.sql.Statement;
import java.sql.Timestamp;
import java.util.Properties;

/** Append-oriented historical store; replayed signal IDs collapse through ReplacingMergeTree. */
public final class ClickHouseHistoricalStore implements RiskSignalStore {
  private static final String TABLE = "risk_signals";
  private final Connection connection;
  private final PreparedStatement insert;

  public ClickHouseHistoricalStore(String jdbcUrl, String username, String password)
      throws IOException {
    Properties properties = new Properties();
    properties.setProperty("user", username);
    properties.setProperty("password", password);
    Connection opened = null;
    try {
      opened = DriverManager.getConnection(jdbcUrl, properties);
      try (Statement statement = opened.createStatement()) {
        statement.execute(
            """
        CREATE TABLE IF NOT EXISTS risk_signals (
          signal_id String, actor_id String, triggering_event_id String, rule_id LowCardinality(String),
          risk_score UInt8, reason String, observed_event_count UInt64, observed_severity_sum UInt64,
          emitted_at DateTime64(3, 'UTC'), stored_at DateTime64(3, 'UTC') DEFAULT now64(3)
        ) ENGINE = ReplacingMergeTree(stored_at) ORDER BY signal_id
        """);
      }
      insert =
          opened.prepareStatement(
              "INSERT INTO "
                  + TABLE
                  + " (signal_id,actor_id,triggering_event_id,rule_id,risk_score,reason,observed_event_count,observed_severity_sum,emitted_at) VALUES (?,?,?,?,?,?,?,?,?)");
      connection = opened;
    } catch (SQLException e) {
      if (opened != null)
        try {
          opened.close();
        } catch (SQLException closeFailure) {
          e.addSuppressed(closeFailure);
        }
      throw new IOException("failed to initialize ClickHouse historical store", e);
    }
  }

  @Override
  public void write(RiskSignal s) throws IOException {
    try {
      insert.setString(1, s.signalId());
      insert.setString(2, s.actorId());
      insert.setString(3, s.triggeringEventId());
      insert.setString(4, s.ruleId());
      insert.setInt(5, s.riskScore());
      insert.setString(6, s.reason());
      insert.setLong(7, s.observedEventCount());
      insert.setLong(8, s.observedSeveritySum());
      insert.setTimestamp(9, Timestamp.from(s.emittedAt()));
      insert.executeUpdate();
    } catch (SQLException e) {
      throw new IOException("failed to persist risk signal " + s.signalId(), e);
    }
  }

  @Override
  public void close() throws IOException {
    try {
      insert.close();
    } catch (SQLException e) {
      try {
        connection.close();
      } catch (SQLException closeFailure) {
        e.addSuppressed(closeFailure);
      }
      throw new IOException("failed to close ClickHouse historical store", e);
    }
    try {
      connection.close();
    } catch (SQLException e) {
      throw new IOException("failed to close ClickHouse historical store", e);
    }
  }
}
