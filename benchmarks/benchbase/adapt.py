"""Apply the PgLite YCSB-only adapter to a clean pinned BenchBase checkout."""
import sys
import subprocess
from pathlib import Path
root = Path(sys.argv[1])
expected = Path(__file__).with_name('upstream-revision.txt').read_text().strip()
actual = subprocess.check_output(['git', '-C', str(root), 'rev-parse', 'HEAD'], text=True).strip()
if actual != expected:
    raise SystemExit(f'Expected BenchBase {expected}; got {actual}')
if subprocess.check_output(['git', '-C', str(root), 'diff', '--name-only'], text=True).strip():
    raise SystemExit('Use a clean BenchBase checkout; adapter is not idempotent')
def replace(path, old, new):
    p = root / path
    s = p.read_text()
    assert old in s, path
    p.write_text(s.replace(old, new))
replace('src/main/java/com/oltpbenchmark/api/BenchmarkModule.java',
'''      this.catalog =
          SQLUtil.getCatalog(this, this.getWorkloadConfiguration().getDatabaseType(), conn);''',
'''      if (conn.isClosed()) throw new SQLException("Connection closed");
      // PgLite adapter: fixed YCSB schema; no PostgreSQL system catalog emulation.
      if (!this.getBenchmarkName().equalsIgnoreCase("ycsb")) {
        throw new SQLException("PgLite adapter supports YCSB only");
      }
      var table = new com.oltpbenchmark.catalog.Table("usertable", "\\\"");
      table.addColumn(new com.oltpbenchmark.catalog.Column(
          "ycsb_key", "\\\"", table, java.sql.Types.INTEGER, 4, false));
      for (int i = 1; i <= 10; i++) {
        table.addColumn(new com.oltpbenchmark.catalog.Column(
            "field" + i, "\\\"", table, java.sql.Types.VARCHAR, 100, true));
      }
      this.catalog = new com.oltpbenchmark.catalog.Catalog(java.util.Map.of("usertable", table));''')
replace('src/main/java/com/oltpbenchmark/benchmarks/ycsb/YCSBBenchmark.java',
'      String userCount = SQLUtil.getMaxColSQL(this.workConf.getDatabaseType(), t, "ycsb_key");',
'      String userCount = "SELECT ycsb_key FROM usertable ORDER BY ycsb_key DESC LIMIT 1";')
p = 'src/main/java/com/oltpbenchmark/api/Worker.java'
replace(p, 'this.conn.setTransactionIsolation(this.configuration.getIsolationMode());',
'''if (this.configuration.getIsolationMode() != Connection.TRANSACTION_REPEATABLE_READ) {
          throw new SQLException("PgLite requires snapshot isolation / REPEATABLE READ");
        }
        try (var s = this.conn.createStatement();
             var r = s.executeQuery("SHOW transaction_isolation")) {
          if (!r.next() || !"repeatable read".equals(r.getString(1))) {
            throw new SQLException("PgLite snapshot isolation not active");
          }
        }
        this.conn.rollback();''')
p = root / 'src/main/java/com/oltpbenchmark/benchmarks/ycsb/YCSBLoader.java'
s = p.read_text()
a = s.index('    Table catalog_tbl =')
s = s[:a] + '''    // Loading only: one atomic multi-row INSERT, safely retryable on lock/WAL pressure.
    for (int base = start; base < stop; base += 64) {
      int rows = Math.min(64, stop - base);
      String tuple = "(?,?,?,?,?,?,?,?,?,?,?)";
      String sql = "INSERT INTO usertable VALUES " +
          String.join(",", java.util.Collections.nCopies(rows, tuple));
      try (PreparedStatement stmt = conn.prepareStatement(sql)) {
        int param = 1;
        for (int row = 0; row < rows; row++) {
          stmt.setInt(param++, base + row);
          for (int field = 0; field < 10; field++) {
            stmt.setString(param++, TextGenerator.randomStr(rng(), benchmark.fieldSize));
          }
        }
        for (int attempt = 0; ; attempt++) {
          try {
            if (stmt.executeUpdate() != rows) throw new SQLException("Incomplete load");
            break;
          } catch (SQLException e) {
            if (attempt >= 100 || !("55P03".equals(e.getSQLState()) ||
                "53100".equals(e.getSQLState()))) throw e;
            try { Thread.sleep(20); }
            catch (InterruptedException interrupted) {
              Thread.currentThread().interrupt();
              throw new SQLException(interrupted);
            }
          }
        }
      }
    }
  }
}
'''
p.write_text(s)

p = root / 'src/main/java/com/oltpbenchmark/benchmarks/ycsb/YCSBBenchmark.java'
s = p.read_text()
s = s.replace('      try (Connection metaConn = this.makeConnection();', '      int init_record_count = 0;\n      try (Connection metaConn = this.makeConnection();')
s = s.replace('        int init_record_count = 0;\n        while (res.next())', '        while (res.next())')
s = s.replace('        for (int i = 0; i < workConf.getTerminals(); ++i) {\n          workers.add(new YCSBWorker(this, i, init_record_count + 1));\n        }\n      }', '      }\n      // Release the setup socket before opening all 32 worker sockets.\n      for (int i = 0; i < workConf.getTerminals(); ++i) {\n        workers.add(new YCSBWorker(this, i, init_record_count + 1));\n      }')
p.write_text(s)
