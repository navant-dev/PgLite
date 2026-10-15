"""Run the modified BenchBase against an already-running isolated PgLite server."""
import argparse
import subprocess
from pathlib import Path
p = argparse.ArgumentParser(description=__doc__)
p.add_argument('--distribution', type=Path, required=True)
p.add_argument('--java', default='java')
p.add_argument('--results', type=Path, required=True)
p.add_argument('--skip-load', action='store_true', help='Reuse a successfully loaded 100,000-row dataset')
p.add_argument('--workloads', nargs='+', choices=['read-1', 'read-8', 'read-32', 'mixed-8', 'scan-8'])
a = p.parse_args()
here = Path(__file__).resolve().parent
a.results = a.results.resolve()
a.results.mkdir(parents=True, exist_ok=False)
subprocess.run(['python3', str(here / 'configure.py')], check=True)
workloads = a.workloads or ['read-1', 'read-8', 'read-32', 'mixed-8', 'scan-8']
if not a.skip_load:
    workloads.insert(0, 'smoke')
for name in workloads:
    print('Running', name, flush=True)
    load = name == 'smoke'
    cmd = [a.java, '-jar', 'benchbase.jar', '-b', 'ycsb', '-c', str(here / 'configs' / (name + '.xml')),
           '--create=' + str(load).lower(), '--load=' + str(load).lower(), '--execute=true',
           '-d', str(a.results / name), '--json-histograms', str(a.results / (name + '.histograms.json'))]
    with (a.results / (name + '.log')).open('w') as log:
        subprocess.run(cmd, cwd=a.distribution, stdout=log, stderr=subprocess.STDOUT, check=True, timeout=300)
    summaries = list((a.results / name).glob('*.summary.json'))
    if len(summaries) != 1:
        raise RuntimeError(f'Missing/ambiguous summary: {name}')
print('Results:', a.results)
