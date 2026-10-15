"""Validate a stopped, checkpointed BenchBase database without modifying it."""
import argparse
import json
import sqlite3
from pathlib import Path

parser = argparse.ArgumentParser(description=__doc__)
parser.add_argument('--database', required=True, type=Path)
parser.add_argument('--output', required=True, type=Path)
args = parser.parse_args()
database = args.database.resolve(strict=True)
wal = Path(str(database) + '-wal')
if wal.exists() and wal.stat().st_size:
    raise SystemExit('Stop the server cleanly and checkpoint WAL before offline validation.')
with sqlite3.connect(database.as_uri() + '?mode=ro&immutable=1', uri=True) as connection:
    bad_fields = ' OR '.join(
        f'field{i} IS NULL OR typeof(field{i}) != \'text\' OR length(field{i}) != 100'
        for i in range(1, 11)
    )
    result = {
        'integrity_check': [row[0] for row in connection.execute('PRAGMA integrity_check')],
        'rows': connection.execute('SELECT count(*) FROM usertable').fetchone()[0],
        'key_range': list(connection.execute('SELECT min(ycsb_key),max(ycsb_key) FROM usertable').fetchone()),
        'invalid_field_rows': connection.execute('SELECT count(*) FROM usertable WHERE ' + bad_fields).fetchone()[0],
        'database_bytes': database.stat().st_size,
    }
result['passed'] = (result['integrity_check'] == ['ok'] and result['rows'] == 100000
                    and result['key_range'] == [0, 99999] and result['invalid_field_rows'] == 0)
args.output.parent.mkdir(parents=True, exist_ok=True)
args.output.write_text(json.dumps(result, indent=2) + '\n')
print(json.dumps(result, indent=2))
if not result['passed']:
    raise SystemExit('BenchBase dataset validation FAILED')
