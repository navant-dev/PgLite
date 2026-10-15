"""Generate reproducible YCSB configurations for the PgLite adapter."""
from pathlib import Path
import xml.etree.ElementTree as E
out = Path(__file__).parent / 'configs'
out.mkdir(exist_ok=True)
for name, clients, weights in [('smoke', 1, '95,0,0,5,0,0'), ('read-1', 1, '100,0,0,0,0,0'), ('read-8', 8, '100,0,0,0,0,0'), ('read-32', 32, '100,0,0,0,0,0'), ('mixed-8', 8, '95,0,0,5,0,0'), ('scan-8', 8, '0,0,100,0,0,0')]:
    r = E.Element('parameters')
    fields = dict(ddlpath=str((Path(__file__).parent / 'schema.sql').resolve()), type='POSTGRES', driver='org.postgresql.Driver', url='jdbc:postgresql://127.0.0.1:55433/pglite?sslmode=disable&assumeMinServerVersion=9.0&ApplicationName=benchbase&reWriteBatchedInserts=true', username='local', password='', isolation='TRANSACTION_REPEATABLE_READ', batchsize='64', scalefactor='100', fieldSize='100', skewFactor='0.99', terminals=str(clients), loaderThreads='1', randomSeed='42', retries='1')
    for k,v in fields.items(): E.SubElement(r,k).text=v
    w = E.SubElement(E.SubElement(r,'works'),'work')
    for k,v in dict(time='5' if name=='smoke' else '30', warmup='2' if name=='smoke' else '10', rate='unlimited', weights=weights).items(): E.SubElement(w,k).text=v
    ts=E.SubElement(r,'transactiontypes')
    for n in ['ReadRecord','InsertRecord','ScanRecord','UpdateRecord','DeleteRecord','ReadModifyWriteRecord']:
        E.SubElement(E.SubElement(ts,'transactiontype'),'name').text=n
    E.indent(r)
    E.ElementTree(r).write(out/f'{name}.xml',encoding='unicode',xml_declaration=True)
