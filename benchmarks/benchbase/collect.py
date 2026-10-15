"""Archive completed BenchBase runs and derive a table from measurement summaries."""
import argparse
import gzip
import json
import shutil
from pathlib import Path
p=argparse.ArgumentParser(description=__doc__)
p.add_argument('sources', nargs='+', type=Path)
p.add_argument('--output', type=Path, required=True)
a=p.parse_args()
a.output.mkdir(parents=True, exist_ok=True)
rows=[]
for source in a.sources:
    for run in sorted(source.iterdir()):
        if not run.is_dir(): continue
        summaries=list(run.glob('*.summary.json'))
        if len(summaries)!=1: continue
        s=json.loads(summaries[0].read_text())
        if s['Final State']!='EXIT' or s['Measured Requests']<=0:
            raise RuntimeError(f'Incomplete run: {run}')
        dest=a.output/run.name
        dest.mkdir(exist_ok=False)
        for f in run.iterdir():
            if f.name.endswith('.raw.csv'):
                (dest/(f.name+'.gz')).write_bytes(gzip.compress(f.read_bytes(),mtime=0))
            elif f.is_file(): shutil.copy2(f,dest/f.name)
        for suffix in ['.log','.histograms.json']:
            f=source/(run.name+suffix)
            if f.exists(): shutil.copy2(f,dest/f.name)
        h=json.loads((source/(run.name+'.histograms.json')).read_text())
        failures=sum(h[k]['NUM_SAMPLES'] for k in ['rejected','aborted','unexpected'])
        latency=s['Latency Distribution']
        rows.append({'workload':run.name,'measured_requests':s['Measured Requests'],
            'elapsed_seconds':s['Elapsed Time (nanoseconds)']/1e9,
            'throughput':s['Throughput (requests/second)'],
            'p50_us':latency['Median Latency (microseconds)'],
            'p95_us':latency['95th Percentile Latency (microseconds)'],
            'p99_us':latency['99th Percentile Latency (microseconds)'],
            'failures_including_warmup':failures})
(a.output/'summary.json').write_text(json.dumps(rows,indent=2)+'\n')
print('| Workload | Measured attempts/s | p50 ms | p95 ms | p99 ms | Failures incl. warmup |')
print('|---|---:|---:|---:|---:|---:|')
for r in rows:
    print(f"| {r['workload']} | {r['throughput']:,.0f} | {r['p50_us']/1000:.3f} | {r['p95_us']/1000:.3f} | {r['p99_us']/1000:.3f} | {r['failures_including_warmup']} |")
