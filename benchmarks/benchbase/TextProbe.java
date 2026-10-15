import java.sql.*;

/** Standalone regression probe; run only against an empty isolated PgLite database. */
public class TextProbe {
  public static void main(String[] args) throws Exception {
    String url = "jdbc:postgresql://127.0.0.1:55433/pglite?sslmode=disable&assumeMinServerVersion=9.0";
    boolean mismatch = false;
    try (Connection c = DriverManager.getConnection(url, "local", "")) {
      c.createStatement().execute("CREATE TABLE text_probe_quotes(id INT PRIMARY KEY, body TEXT)");
      String[] values = {"a''b", "plain", "a\\b", "'quoted'", "a\\nb", "a\\\\b", ""};
      try (PreparedStatement insert = c.prepareStatement("INSERT INTO text_probe_quotes VALUES (?,?)");
           PreparedStatement read = c.prepareStatement("SELECT body FROM text_probe_quotes WHERE id=?")) {
        for (int i=0; i<values.length; i++) {
          insert.setInt(1,i); insert.setString(2,values[i]); insert.executeUpdate();
          read.setInt(1,i);
          try (ResultSet r = read.executeQuery()) {
            if (!r.next()) throw new SQLException("Missing probe row");
            String actual = r.getString(1);
            boolean equal = values[i].equals(actual);
            System.out.printf("case=%d equal=%s expected_hex=%s actual_hex=%s%n", i, equal,
                java.util.HexFormat.of().formatHex(values[i].getBytes(java.nio.charset.StandardCharsets.UTF_8)),
                java.util.HexFormat.of().formatHex(actual.getBytes(java.nio.charset.StandardCharsets.UTF_8)));
            mismatch |= !equal;
          }
        }
      }
    }
    if (mismatch) throw new AssertionError("PgLite did not preserve JDBC text parameters");
  }
}
