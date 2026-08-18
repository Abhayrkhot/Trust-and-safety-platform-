package dev.trustsafety.sink;

import static org.assertj.core.api.Assertions.assertThat;
import java.sql.DriverManager;
import org.junit.jupiter.api.Test;
import org.testcontainers.clickhouse.ClickHouseContainer;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;
import org.testcontainers.utility.DockerImageName;

@Testcontainers
class ClickHouseHistoricalStoreIT {
  @Container static final ClickHouseContainer CLICKHOUSE=new ClickHouseContainer(DockerImageName.parse("clickhouse/clickhouse-server:25.8-alpine"));
  @Test void persistsFieldsAndCollapsesReplayedSignalId() throws Exception {
    var signal=RiskSignalSinkTest.signal("same-id",2_000);
    try(var store=new ClickHouseHistoricalStore(CLICKHOUSE.getJdbcUrl(),CLICKHOUSE.getUsername(),CLICKHOUSE.getPassword())){store.write(signal);store.write(signal);}
    try(var connection=DriverManager.getConnection(CLICKHOUSE.getJdbcUrl(),CLICKHOUSE.getUsername(),CLICKHOUSE.getPassword());var statement=connection.createStatement();var rows=statement.executeQuery("SELECT signal_id,actor_id,risk_score FROM risk_signals FINAL")){
      assertThat(rows.next()).isTrue();assertThat(rows.getString(1)).isEqualTo("same-id");assertThat(rows.getString(2)).isEqualTo("actor");assertThat(rows.getInt(3)).isEqualTo(80);assertThat(rows.next()).isFalse();
    }
  }
}
