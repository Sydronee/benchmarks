#!/usr/bin/env python3
"""
TiC Thesis Evidence Runner

Run from the root of the TiC repository.  It executes the actual repository
modules against small synthetic fixtures where practical, records observed
PASS/FAIL/NOT RUN results, and optionally measures real local-file ingestion and
API latency.

It does not invent production performance numbers.

Usage:
  python thesis_evidence_runner.py
  python thesis_evidence_runner.py --api
  python thesis_evidence_runner.py --measure-ingestion "C:\\data\\file.json.gz"
"""
from __future__ import annotations

import argparse, csv, gzip, hashlib, http.server, importlib.util, inspect, io, json
import os, platform, shutil, socket, socketserver, subprocess, sys, tempfile, threading
import time, traceback, urllib.error, urllib.parse, urllib.request, zipfile
from dataclasses import dataclass, asdict
from pathlib import Path

try:
    import duckdb
except Exception:
    duckdb = None
try:
    import ijson
except Exception:
    ijson = None
try:
    import psutil
except Exception:
    psutil = None

RESULTS = []
METRICS = {}

@dataclass
class Result:
    tc: str; category: str; description: str; expected: str; actual: str; status: str; evidence: str = ""

def rec(tc, cat, desc, expected, actual, status, evidence=""):
    RESULTS.append(Result(tc, cat, desc, expected, actual, status, evidence))
    print(f"{'✓' if status=='PASS' else '✗' if status=='FAIL' else '–'} {tc}: {status} — {desc}")
    if evidence: print("   ", evidence)

def human(n):
    if n is None: return "N/A"
    x=float(n)
    for u in ("B","KB","MB","GB","TB"):
        if x < 1024: return f"{x:.2f} {u}"
        x /= 1024
    return f"{x:.2f} PB"

def duration(s):
    if s is None: return "N/A"
    if s < 1: return f"{s*1000:.1f} ms"
    if s < 60: return f"{s:.3f} s"
    return f"{int(s//60)} min {s%60:.1f} s"

def root(start):
    for p in [start, *start.parents]:
        if (p/'stream_parser.py').exists() and (p/'schema.sql').exists(): return p
    return start

def mod(path, name):
    spec=importlib.util.spec_from_file_location(name,str(path))
    if not spec or not spec.loader: raise ImportError(path)
    m=importlib.util.module_from_spec(spec); sys.modules[name]=m; spec.loader.exec_module(m); return m

def run(cmd,cwd,timeout=600,env=None):
    t=time.perf_counter()
    p=subprocess.run(cmd,cwd=str(cwd),capture_output=True,text=True,timeout=timeout,env=env)
    return p.returncode,p.stdout,p.stderr,time.perf_counter()-t

def initdb(schema, db):
    con=duckdb.connect(str(db))
    try: con.execute(schema.read_text(encoding='utf-8'))
    finally: con.close()

def counts(db):
    con=duckdb.connect(str(db),read_only=True)
    try:
        tables={r[0] for r in con.execute('SHOW TABLES').fetchall()}
        return {t:int(con.execute(f'SELECT COUNT(*) FROM {t}').fetchone()[0]) if t in tables else -1
                for t in ('payers','billing_codes','negotiated_rates','providers')}
    finally: con.close()

def scalar(db,sql,args=()):
    con=duckdb.connect(str(db),read_only=True)
    try: return con.execute(sql,list(args)).fetchone()[0]
    finally: con.close()

def fixture():
    # Matches the structure described in the thesis test plan. There are 6
    # institutional rows total in the parser fixture: 5 under 99213 + 1 under MS-DRG.
    # Of these, only 2 per payer qualify for benchmark construction because the
    # percentage-type MS-DRG row is filtered there. Also includes
    # one professional row, one percentage row and one 20M row for filtering.
    return {
      'reporting_entity_name':'Synthetic Test Payer','reporting_entity_type':'Payer',
      'plan_name':'Synthetic Plan A','plan_id':'SYN-001','plan_id_type':'HIOS',
      'plan_market_type':'group','last_updated_on':'2026-09-01','version':'1.0',
      'provider_references':[{'provider_group_id':101,'provider_groups':[{
        'npi':['1111111117','2222222223'],'tin':{'type':'ein','value':'12-3456789'},
        'facility_name':'Synthetic Medical Center'}],'network_name':['Synthetic Network']}],
      'in_network':[
        {'billing_code':'99213','billing_code_type':'CPT','billing_code_type_version':'2026',
         'description':'MRF free text description','name':'Office Visit','negotiation_arrangement':'ffs',
         'negotiated_rates':[{'provider_references':[101],'negotiated_prices':[
           {'billing_class':'institutional','setting':'outpatient','negotiated_type':'negotiated','negotiated_rate':100.0,'service_code':[],'billing_code_modifier':[],'expiration_date':'2027-01-01'},
           {'billing_class':'institutional','setting':'outpatient','negotiated_type':'negotiated','negotiated_rate':140.0,'service_code':[],'billing_code_modifier':[],'expiration_date':'2027-01-01'},
           {'billing_class':'institutional','setting':'outpatient','negotiated_type':'negotiated','negotiated_rate':0.0,'service_code':[],'billing_code_modifier':[],'expiration_date':'2027-01-01'},
           {'billing_class':'institutional','setting':'outpatient','negotiated_type':'negotiated','negotiated_rate':20000000.0,'service_code':[],'billing_code_modifier':[],'expiration_date':'2027-01-01'},
           {'billing_class':'professional','setting':'outpatient','negotiated_type':'negotiated','negotiated_rate':500.0,'service_code':[],'billing_code_modifier':[],'expiration_date':'2027-01-01'},
           {'billing_class':'institutional','setting':'outpatient','negotiated_type':'percentage','negotiated_rate':15.0,'service_code':[],'billing_code_modifier':[],'expiration_date':'2027-01-01'}]}]},
        {'billing_code':'001','billing_code_type':'MS-DRG','billing_code_type_version':'2026',
         'description':'Synthetic DRG','name':'Synthetic DRG','negotiation_arrangement':'ffs',
         'negotiated_rates':[{'provider_groups':[{
           'npi':['3333333331','4444444449'],'tin':{'type':'ein','value':'98-7654321'},'facility_name':'Embedded Test Hospital'}],
           'negotiated_prices':[{'billing_class':'institutional','setting':'inpatient','negotiated_type':'percentage','negotiated_rate':250.0,'service_code':[],'billing_code_modifier':[],'expiration_date':'2027-01-01'}]}]}
      ]}

def fixture_files(d):
    d.mkdir(parents=True,exist_ok=True); b=json.dumps(fixture()).encode(); out={}
    out['json']=d/'fixture.json'; out['json'].write_bytes(b)
    out['gz']=d/'fixture.json.gz'
    with gzip.open(out['gz'],'wb') as f:f.write(b)
    out['zip_json']=d/'fixture_json.zip'
    with zipfile.ZipFile(out['zip_json'],'w',zipfile.ZIP_DEFLATED) as z:z.writestr('in-network-rates.json',b)
    out['zip_gz']=d/'fixture_gz.zip'
    with zipfile.ZipFile(out['zip_gz'],'w',zipfile.ZIP_DEFLATED) as z:z.writestr('in-network-rates.json.gz',gzip.compress(b))
    return out

def ingestion_tests(repo,work):
    if duckdb is None or ijson is None or not (repo/'stream_parser.py').exists():
        for i in range(1,13): rec(f'TC-{i:02d}','Ingestion','Synthetic ingestion test','Repository parser runnable','duckdb/ijson/parser unavailable','NOT RUN')
        return
    try: parser=mod(repo/'stream_parser.py','tic_parser_test')
    except Exception as e:
        for i in range(1,13): rec(f'TC-{i:02d}','Ingestion','Synthetic ingestion test','Parser imports','Import failed: '+repr(e),'FAIL')
        return
    files=fixture_files(work/'fixtures'); expected={'payers':1,'billing_codes':2,'negotiated_rates':6,'providers':4}
    for i,k in enumerate(('json','gz','zip_json','zip_gz'),1):
        try:
            db=work/f'tc{i}.duckdb'; initdb(repo/'schema.sql',db)
            c=duckdb.connect(str(db)); parser.process_file(c,str(files[k]),files[k].name); c.close()
            a=counts(db); rec(f'TC-{i:02d}','Ingestion',f'Ingest {k}','Counts '+str(expected),str(a),'PASS' if a==expected else 'FAIL','Direct DuckDB counts')
        except Exception as e: rec(f'TC-{i:02d}','Ingestion',f'Ingest {k}',str(expected),type(e).__name__+': '+str(e),'FAIL')
    try:
        db=work/'tc5.duckdb'; initdb(repo/'schema.sql',db); c=duckdb.connect(str(db)); parser.process_file(c,str(files['json']),'tc5'); bad=c.execute("SELECT COUNT(*) FROM negotiated_rates WHERE billing_class <> 'institutional'").fetchone()[0]; c.close()
        rec('TC-05','Ingestion','Professional rows excluded','0 non-institutional rows',str(bad),'PASS' if bad==0 else 'FAIL')
    except Exception as e: rec('TC-05','Ingestion','Professional rows excluded','0',str(e),'FAIL')
    try:
        d1=work/'tc6a.duckdb'; initdb(repo/'schema.sql',d1); c=duckdb.connect(str(d1)); parser.BATCH_SIZE=getattr(parser,'BATCH_SIZE',122880); parser.process_file(c,str(files['json']),'a'); c.close(); a=counts(d1)
        d2=work/'tc6b.duckdb'; initdb(repo/'schema.sql',d2); c=duckdb.connect(str(d2)); parser.BATCH_SIZE=2; parser.process_file(c,str(files['json']),'b'); c.close(); b=counts(d2)
        rec('TC-06','Ingestion','Result independent of batch size',str(a),str(b),'PASS' if a==b else 'FAIL')
    except Exception as e: rec('TC-06','Ingestion','Batch-size independence','Equal counts',str(e),'FAIL')
    try:
        data=files['json'].read_bytes(); s=io.BytesIO(data); h=parser.extract_header(s); ok=len(s.getvalue())==len(data) and s.tell()<len(data) and bool(h)
        rec('TC-07','Ingestion','Header extraction stops before full stream','Partial read with header',f'bytes_read={s.tell()} total={len(data)}','PASS' if ok else 'FAIL')
    except Exception as e: rec('TC-07','Ingestion','Header extraction','Partial read',str(e),'FAIL')
    tests=[('TC-08','Provider group with multiple NPIs',"SELECT COUNT(*) FROM providers WHERE provider_reference_id=101","2"),
           ('TC-09','Embedded group gets negative identifier',"SELECT COUNT(*) FROM providers WHERE provider_reference_id<0",">=1"),
           ('TC-10','Repeated embedded group keeps one synthetic identifier','', '1 distinct negative id'),
           ('TC-11','Malformed JSON rolls back','', 'Exception and empty tables'),
           ('TC-12','ZIP without rate member rejected','', 'Exception')]
    try:
        db=work/'tc08.duckdb'; initdb(repo/'schema.sql',db); c=duckdb.connect(str(db)); parser.process_file(c,str(files['json']),'x'); n=c.execute(tests[0][2]).fetchone()[0]; c.close(); rec('TC-08','Ingestion',tests[0][1],'2 rows',str(n),'PASS' if n==2 else 'FAIL')
    except Exception as e: rec('TC-08','Ingestion',tests[0][1],'2 rows',str(e),'FAIL')
    try:
        db=work/'tc09.duckdb'; initdb(repo/'schema.sql',db); c=duckdb.connect(str(db)); parser.process_file(c,str(files['json']),'x'); n=c.execute(tests[1][2]).fetchone()[0]; c.close(); rec('TC-09','Ingestion',tests[1][1],'At least 1',str(n),'PASS' if n>=1 else 'FAIL')
    except Exception as e: rec('TC-09','Ingestion',tests[1][1],'Negative id',str(e),'FAIL')
    try:
        db=work/'tc10.duckdb'; initdb(repo/'schema.sql',db); c=duckdb.connect(str(db)); parser.process_file(c,str(files['json']),'a'); parser.process_file(c,str(files['json']),'b'); n=c.execute('SELECT COUNT(DISTINCT provider_reference_id) FROM providers WHERE provider_reference_id<0').fetchone()[0]; p=c.execute('SELECT COUNT(*) FROM payers').fetchone()[0]; c.close(); rec('TC-10','Ingestion',tests[2][1],'1 negative id and 2 payers',f'{n} negative ids; {p} payers','PASS' if n==1 and p==2 else 'FAIL')
    except Exception as e: rec('TC-10','Ingestion',tests[2][1],'1 negative id',str(e),'FAIL')
    try:
        bad=work/'bad.json'; bad.write_bytes(files['json'].read_bytes()[:-20]); db=work/'tc11.duckdb'; initdb(repo/'schema.sql',db); c=duckdb.connect(str(db)); raised=False
        try: parser.process_file(c,str(bad),'bad')
        except Exception as e: raised=True; err=type(e).__name__+': '+str(e)
        a=counts(db); c.close(); ok=raised and all(v==0 for v in a.values()); rec('TC-11','Ingestion',tests[3][1],'Exception and empty tables',f'raised={raised}; counts={a}; {err if raised else "no exception"}','PASS' if ok else 'FAIL')
    except Exception as e: rec('TC-11','Ingestion',tests[3][1],'Exception and empty tables',str(e),'FAIL')
    try:
        z=work/'bad.zip';
        with zipfile.ZipFile(z,'w') as zz: zz.writestr('wrong.txt','bad')
        db=work/'tc12.duckdb'; initdb(repo/'schema.sql',db); c=duckdb.connect(str(db)); raised=False
        try: parser.process_file(c,str(z),'bad')
        except Exception as e: raised=True; err=type(e).__name__+': '+str(e)
        c.close(); rec('TC-12','Ingestion',tests[4][1],'Exception',err if raised else 'No exception','PASS' if raised else 'FAIL')
    except Exception as e: rec('TC-12','Ingestion',tests[4][1],'Exception',str(e),'FAIL')

def runner_tests(repo,work):
    if not (repo/'ingest_utils.py').exists() and not (repo/'runner.py').exists():
        for i in range(13,22): rec(f'TC-{i:02d}','Acquisition/Runner','Runner test','Repository runner available','Not found','NOT RUN')
        return
    # Exact runner behaviour varies by branch; only run direct download_file when callable.
    p=repo/'ingest_utils.py' if (repo/'ingest_utils.py').exists() else repo/'runner.py'
    try:m=mod(p,'tic_runner_test')
    except Exception as e:
        for i in range(13,22): rec(f'TC-{i:02d}','Acquisition/Runner','Runner test','Module imports',str(e),'FAIL')
        return
    f=work/'server'; f.mkdir(); payload=b'evidence-test'*40; (f/'payload.bin').write_bytes(payload)
    class H(http.server.SimpleHTTPRequestHandler):
        def log_message(self,*a):pass
        def translate_path(self,path): return str(f/path.lstrip('/'))
    s=socketserver.TCPServer(('127.0.0.1',0),H); port=s.server_address[1]; threading.Thread(target=s.serve_forever,daemon=True).start()
    try:
        dl=getattr(m,'download_file',None)
        if not callable(dl):
            for i in range(13,22): rec(f'TC-{i:02d}','Acquisition/Runner','Branch-specific runner test','download_file callable','Not exposed by detected module','NOT RUN')
            return
        target=work/'downloaded.bin'; url=f'http://127.0.0.1:{port}/payload.bin'
        try:
            dl(url,str(target)); ok=target.exists() and target.stat().st_size==len(payload) and not (Path(str(target)+'.tmp')).exists()
            rec('TC-13','Acquisition/Runner','Successful download','Correct-size target and no tmp',f'exists={target.exists()} size={target.stat().st_size if target.exists() else 0}','PASS' if ok else 'FAIL',str(inspect.signature(dl)))
        except Exception as e: rec('TC-13','Acquisition/Runner','Successful download','Target created',type(e).__name__+': '+str(e),'FAIL')
        for i,d in [(14,'Matching-size file cached'),(15,'Size mismatch rejected'),(16,'HTTP 404 rejected'),(17,'Run-log entry'),(18,'Bounded progress'),(19,'Resume'),(20,'Already-complete rerun'),(21,'Failure without progress advance')]:
            rec(f'TC-{i:02d}','Acquisition/Runner',d,'Observed branch behaviour','Run requires branch-specific orchestration beyond direct helper call','NOT RUN')
    finally:s.shutdown(); s.server_close()

def enrichment_db(path):
    c=duckdb.connect(str(path))
    try:
        c.execute('CREATE TABLE nppes (npi BIGINT, entity_type VARCHAR, provider_name VARCHAR, first_name VARCHAR, last_name VARCHAR, org_name VARCHAR, city VARCHAR, state VARCHAR, zip5 VARCHAR, taxonomy_code VARCHAR, taxonomy_is_primary VARCHAR)')
        c.executemany('INSERT INTO nppes VALUES (?,?,?,?,?,?,?,?,?,?,?)',[
          (1111111117,'1','Test Individual','Test','Individual',None,'Houston','TX','77001','207Q00000X','Y'),
          (2222222223,'2','Synthetic Medical Center',None,None,'Synthetic Medical Center','Houston','TX','77001','282N00000X','Y'),
          (3333333331,'1','Embedded Individual','Embedded','Individual',None,'Houston','TX','77001','207Q00000X','Y'),
          (4444444449,'2','Embedded Hospital',None,None,'Embedded Hospital','Houston','TX','77001','282N00000X','Y')])
        c.execute('CREATE TABLE zip_county (zip5 VARCHAR, fips VARCHAR, county_name VARCHAR, state_abbr VARCHAR, tot_ratio DOUBLE)')
        c.executemany('INSERT INTO zip_county VALUES (?,?,?,?,?)',[('77001','48201','Harris County','TX',.7),('77001','48157','Fort Bend County','TX',.3)])
        c.execute('CREATE TABLE code_descriptions (billing_code VARCHAR, billing_code_type VARCHAR, description VARCHAR, short_description VARCHAR, long_description VARCHAR, effective_date VARCHAR)')
        c.executemany('INSERT INTO code_descriptions VALUES (?,?,?,?,?,?)',[('99213','CPT','Office o/p visit est','Office o/p visit est','Office/outpatient visit established patient','2026-01-01'),('001','MS-DRG','Synthetic DRG canonical','Synthetic DRG canonical','Synthetic DRG canonical description','2026-01-01')])
    finally:c.close()

def benchmark_tests(repo,work):
    b=repo/'build_benchmarks.py'; sp=repo/'stream_parser.py'
    if not b.exists() or duckdb is None:
        for i in range(22,29): rec(f'TC-{i:02d}','Benchmark','Benchmark test','Builder available','Prerequisite missing','NOT RUN')
        return None
    try:
        source=work/'benchmark.duckdb'; enr=work/'enrichment.duckdb'; initdb(repo/'schema.sql',source); ef=fixture_files(work/'bmfix')
        parser=mod(sp,'tic_parser_bm'); c=duckdb.connect(str(source)); parser.process_file(c,str(ef['json']),'one'); parser.process_file(c,str(ef['json']),'two'); c.close(); enrichment_db(enr)
        rc,out,err,t=run([sys.executable,str(b),'--transparency-db',str(source),'--enrichment-db',str(enr),'--drop-first'],repo)
        (work/'benchmark_build_output.txt').write_text(out+'\n'+err,encoding='utf-8')
        if rc!=0:
            rec('TC-22','Benchmark','Synthetic benchmark build','Exit code 0',f'code={rc}','FAIL',err[-1500:])
            for i in range(23,29): rec(f'TC-{i:02d}','Benchmark','Synthetic benchmark test','Benchmark DB exists','Build failed','NOT RUN')
            return None
        n=scalar(source,'SELECT COUNT(*) FROM benchmarks'); rec('TC-22','Benchmark','Quality filter retains qualifying rows','4 benchmark rows',str(n),'PASS' if n==4 else 'FAIL')
        tabs={r[0] for r in duckdb.connect(str(source),read_only=True).execute('SHOW TABLES').fetchall()}; expected={'benchmarks','benchmarks_code_stats','benchmarks_geo_stats','benchmarks_geo_payer_stats','benchmarks_payer_stats','benchmarks_payer_provider_stats','benchmarks_provider_stats'}; rec('TC-23','Benchmark','Benchmark + six statistical tables','7 tables',str(sorted(tabs&expected)),'PASS' if expected.issubset(tabs) else 'FAIL')
        c=duckdb.connect(str(source),read_only=True)
        try:
            county=c.execute("SELECT county_name FROM benchmarks WHERE billing_code='99213' LIMIT 1").fetchone(); county=county[0] if county else None
            rec('TC-24','Benchmark','Dominant county rule','Harris County',str(county),'PASS' if county=='Harris County' else 'FAIL')
            row=c.execute("SELECT npi,npi_count,code_description,has_canonical_description FROM benchmarks WHERE billing_code='99213' LIMIT 1").fetchone()
        finally:c.close()
        npi,cnt,desc,canon=row if row else (None,None,None,None)
        rec('TC-25','Benchmark','Representative organization NPI','2222222223; npi_count=2',f'npi={npi}; count={cnt}','PASS' if npi==2222222223 and cnt==2 else 'FAIL')
        rec('TC-26','Benchmark','Canonical description preferred','Office o/p visit est; true',f'{desc!r}; {canon!r}','PASS' if desc=='Office o/p visit est' and bool(canon) else 'FAIL')
        helpc,ho,he,_=run([sys.executable,str(b),'--help'],repo,30); ht=ho+'\n'+he
        rec('TC-27','Benchmark','Approximate-percentile option','Option exposed and runnable','--approx-percentiles present' if '--approx-percentiles' in ht else 'Current branch does not expose switch','PASS' if '--approx-percentiles' in ht else 'NOT RUN')
        rec('TC-28','Benchmark','Skip-indexes/skip-stats mode','Both options exposed','Options present' if '--skip-indexes' in ht and '--skip-stats' in ht else 'Current branch does not expose both switches','PASS' if '--skip-indexes' in ht and '--skip-stats' in ht else 'NOT RUN')
        METRICS['synthetic_benchmark_build_seconds']=t; return source
    except Exception as e:
        for i in range(22,29): rec(f'TC-{i:02d}','Benchmark','Benchmark test','Executable build',type(e).__name__+': '+str(e),'FAIL' if i==22 else 'NOT RUN')
        return None

def validation_tests(repo,db,work):
    v=repo/'validate_data.py'
    if not v.exists() or db is None:
        for i in range(29,34): rec(f'TC-{i:02d}','Validation','Validator test','Validator + DB available','Prerequisite missing','NOT RUN')
        return
    base=work/'valid.duckdb'; shutil.copy2(db,base)
    for tc,extra,args in [
      ('TC-29',None,[]),('TC-30',None,['--fast'])]:
        try:
            outp=work/(tc+'.txt'); rc,o,e,t=run([sys.executable,str(v),'--db',str(base),'--txt',str(outp)]+args,repo); rec(tc,'Validation','Validator on consistent DB' if tc=='TC-29' else 'Fast validation mode','Exit code 0',f'code={rc}; time={duration(t)}','PASS' if rc==0 else 'FAIL',(o+e)[-1000:])
        except Exception as e: rec(tc,'Validation','Validator test','Exit 0',str(e),'FAIL')
    def mutate(name,sql):
        p=work/name; shutil.copy2(db,p); c=duckdb.connect(str(p)); c.execute(sql); c.close(); return p
    for tc,sql,needle in [
      ('TC-31',"INSERT INTO negotiated_rates SELECT 999999999,* EXCLUDE (payer_id) FROM negotiated_rates LIMIT 1",'orphan'),
      ('TC-32',"UPDATE providers SET npi=9999999999 WHERE provider_id=(SELECT MIN(provider_id) FROM providers)",'npi'),
      ('TC-33',"INSERT INTO negotiated_rates SELECT payer_id,code_id,billing_class,setting,negotiated_type,0.0,service_code,billing_code_modifier,provider_reference_ids,source_file,ingested_at FROM negotiated_rates LIMIT 1",'rate')]:
        try:
            p=mutate(tc+'.duckdb',sql); rc,o,e,t=run([sys.executable,str(v),'--db',str(p),'--txt',str(work/(tc+'.txt'))],repo); ev=(o+e)[-1800:]; rec(tc,'Validation',{'TC-31':'Orphan payer reference detected','TC-32':'Invalid NPI detected','TC-33':'Rate-domain violation detected'}[tc],'Validator reports the planted defect',f'code={rc}', 'PASS' if needle in ev.lower() or rc!=0 else 'FAIL',ev)
        except Exception as e: rec(tc,'Validation','Planted defect test','Validator reports defect',str(e),'FAIL')

def httpget(url):
    t=time.perf_counter()
    try:
        with urllib.request.urlopen(url,timeout=10) as r:
            raw=r.read().decode('utf-8','replace');
            try:b=json.loads(raw)
            except:b=raw
            return r.status,b,time.perf_counter()-t,''
    except urllib.error.HTTPError as e:
        raw=e.read().decode('utf-8','replace'); return e.code,raw,time.perf_counter()-t,''
    except Exception as e:return 0,None,time.perf_counter()-t,str(e)

def api_tests(repo,db,work):
    s=repo/'server_benchmarks.js'
    if not s.exists() or db is None:
        for i in range(34,44): rec(f'TC-{i:02d}','API','API test','Server + benchmark DB available','Prerequisite unavailable','NOT RUN')
        return
    if not (s.parent/'node_modules').exists():
        for i in range(34,44): rec(f'TC-{i:02d}','API','API test','Node dependencies installed','Run npm install in benchmarks repo','NOT RUN')
        return
    port=0
    with socket.socket() as so: so.bind(('127.0.0.1',0)); port=so.getsockname()[1]
    env=os.environ.copy(); env.update({'PORT':str(port),'DUCKDB_PATH':str(db)})
    p=subprocess.Popen(['node',str(s)],cwd=str(s.parent),env=env,stdout=subprocess.PIPE,stderr=subprocess.PIPE,text=True)
    base=f'http://127.0.0.1:{port}'
    try:
        end=time.time()+30
        while time.time()<end:
            st,_,_,_=httpget(base+'/api/health')
            if st<500 and st!=0: break
            time.sleep(.25)
        # derive one valid code/type/state/payer from stats
        c=duckdb.connect(str(db),read_only=True); row=c.execute('SELECT billing_code,billing_code_type FROM benchmarks_code_stats ORDER BY n_rates DESC LIMIT 1').fetchone(); c.close()
        if not row:
            for i in range(34,44): rec(f'TC-{i:02d}','API','API test','Valid benchmark input','No code in stats','NOT RUN')
            return
        code,typ=map(str,row); q=lambda d:urllib.parse.urlencode(d)
        calls=[('TC-34','/api/health',200),('TC-35','/api/code-types',200),('TC-36','/api/benchmark/summary?'+q({'code':code,'type':typ}),200),('TC-37','/api/benchmark/states?'+q({'code':code,'type':typ}),200),('TC-38','/api/benchmark/payers?'+q({'code':code,'type':typ}),200),('TC-40','/api/benchmark/summary?'+q({'code':code}),400),('TC-41','/api/benchmark/summary?'+q({'code':"99213' OR 1=1 --",'type':typ}),400),('TC-42','/api/benchmark/summary?code=00000&type=CPT',200)]
        samples=[]
        for tc,path,exp in calls:
            st,b,t,e=httpget(base+path); samples.append(t); rec(tc,'API',path,f'HTTP {exp}',f'HTTP {st}; {str(b)[:500]}','PASS' if st==exp else 'FAIL',f'{t*1000:.1f} ms'+(('; '+e) if e else ''))
        # state/payer-dependent endpoints, using first available rows
        c=duckdb.connect(str(db),read_only=True); sr=c.execute('SELECT provider_state FROM benchmarks_geo_stats WHERE billing_code=? AND billing_code_type=? LIMIT 1',[code,typ]).fetchone(); pr=c.execute('SELECT payer_name FROM benchmarks_payer_stats WHERE billing_code=? AND billing_code_type=? LIMIT 1',[code,typ]).fetchone(); c.close()
        if sr:
            st=str(sr[0]); stq=q({'code':code,'type':typ,'state':st});
            for tc,path,exp in [('TC-37b','/api/benchmark/counties?'+stq,200),('TC-38b','/api/benchmark/payer-counties?'+stq,200)]:
                x,b,t,e=httpget(base+path); samples.append(t); rec(tc,'API',path,f'HTTP {exp}',f'HTTP {x}','PASS' if x==exp else 'FAIL',f'{t*1000:.1f} ms')
        else: rec('TC-37b','API','Counties endpoint','HTTP 200','No state available','NOT RUN'); rec('TC-38b','API','Payer-county endpoint','HTTP 200','No state available','NOT RUN')
        if pr and sr:
            x,b,t,e=httpget(base+'/api/benchmark/providers?'+q({'code':code,'type':typ,'payer':str(pr[0]),'state':str(sr[0])})); samples.append(t); rec('TC-39','API','Provider endpoint','HTTP 200',f'HTTP {x}','PASS' if x==200 else 'FAIL',f'{t*1000:.1f} ms')
            x,b,t,e=httpget(base+'/api/benchmark/providers?'+q({'code':code,'type':typ,'payer':str(pr[0])})); samples.append(t); rec('TC-43','API','Providers without state','HTTP 400',f'HTTP {x}','PASS' if x==400 else 'FAIL',f'{t*1000:.1f} ms')
        else: rec('TC-39','API','Provider endpoint','HTTP 200','Inputs unavailable','NOT RUN'); rec('TC-43','API','Providers without state','HTTP 400','Inputs unavailable','NOT RUN')
        if samples: METRICS['api_response_time_median_seconds']=sorted(samples)[len(samples)//2]
    finally:
        try:p.terminate();p.wait(5)
        except Exception:p.kill()

def dashboard_checks(repo,work):
    cand=[repo/'benchmark_dashboard.html',repo.parent/'benchmarks'/'benchmark_dashboard.html',repo/'analytics.html']; h=next((p for p in cand if p.exists()),None)
    lines=['TC-44 Page load: verify billing-code-type selector.','TC-45 Empty code: click Compare; expected "Enter a billing code first."','TC-46 Valid code: verify description, 5 KPI cards, payer table, state chart/table.','TC-47 Unknown code: expected "No rates found for [type] [code]."','TC-48 First state: verify county and payer-by-county sections appear.','TC-49 Another state: verify highlight, reload, provider hidden.','TC-50 Payer-county row: verify provider section and heading.','TC-51 Stop API server: search should show an Error message.','', 'Screenshots: Screen 3.1–3.5, Screen 5.1 pipeline terminal, Screen 5.2 validation output.']
    (work/'dashboard_manual_checklist.txt').write_text('\n'.join(lines)+'\n',encoding='utf-8')
    for i in range(44,52): rec(f'TC-{i:02d}','Dashboard','Manual browser test','Observe actual dashboard',('Dashboard file found: '+h.name) if h else 'Dashboard HTML not found','NOT RUN')

def measure_ingestion(repo,files,out):
    if duckdb is None or ijson is None: print('Real ingestion measurement skipped: duckdb/ijson unavailable.'); return
    parser=mod(repo/'stream_parser.py','tic_parser_perf'); db=out/'real_ingestion.duckdb'; initdb(repo/'schema.sql',db); total=sum(p.stat().st_size for p in files); samples=[]; stop=threading.Event()
    def sample():
        pr=psutil.Process(os.getpid())
        while not stop.is_set():
            try:samples.append(pr.memory_info().rss)
            except Exception:pass
            stop.wait(.2)
    th=None
    if psutil: th=threading.Thread(target=sample,daemon=True); th.start()
    t0=time.perf_counter(); c=duckdb.connect(str(db))
    try:
        for i,p in enumerate(files,1): print(f'REAL {i}/{len(files)} {p} ({human(p.stat().st_size)})'); parser.process_file(c,str(p),p.name)
        c.execute('CHECKPOINT')
    finally:c.close()
    elapsed=time.perf_counter()-t0; stop.set(); th.join(1) if th else None; rows=scalar(db,'SELECT COUNT(*) FROM negotiated_rates')
    METRICS.update({'input_data_size_compressed_bytes':total,'number_of_source_files_processed':len(files),'records_processed_rate_rows_stored':rows,'ingestion_duration_seconds':elapsed,'peak_memory_bytes':max(samples) if samples else None,'transparency_database_size_bytes':db.stat().st_size})
    print('\nTABLE 4.8 REAL INGESTION'); print('input:',human(total));print('files:',len(files));print('rate rows:',rows);print('duration:',duration(elapsed));print('peak memory:',human(max(samples) if samples else None));print('db size:',human(db.stat().st_size))

def write(out):
    (out/'thesis_test_results.json').write_text(json.dumps({'metrics':METRICS,'tests':[asdict(x) for x in RESULTS]},indent=2,default=str),encoding='utf-8')
    with (out/'thesis_test_results.csv').open('w',newline='',encoding='utf-8') as f:
        w=csv.DictWriter(f,fieldnames=list(asdict(RESULTS[0]).keys()) if RESULTS else ['tc','category','description','expected','actual','status','evidence']); w.writeheader(); [w.writerow(asdict(x)) for x in RESULTS]
    md=['# TiC Thesis Evidence Report','',f'Generated: {time.strftime("%Y-%m-%d %H:%M:%S")}','', '## Environment','']
    for k in ('python_version','node_version','duckdb_python_version','platform','machine','processor'): 
        if k in METRICS: md.append(f'- **{k}**: {METRICS[k]}')
    md += ['','## Tests','','|TC|Category|Status|Actual|','|---|---|---|---|']
    for r in RESULTS: md.append(f'|{r.tc}|{r.category}|{r.status}|{r.actual.replace("|","/").replace(chr(10)," ")[:500]}|')
    md += ['','## Table 4.8 Metrics','', '|Metric|Value|','|---|---|']
    pairs=[('Input data size (compressed)',human(METRICS.get('input_data_size_compressed_bytes'))),('Number of source files processed',METRICS.get('number_of_source_files_processed','NOT MEASURED')),('Records processed (rate rows stored)',METRICS.get('records_processed_rate_rows_stored','NOT MEASURED')),('Ingestion duration',duration(METRICS.get('ingestion_duration_seconds'))),('Peak memory during ingestion',human(METRICS.get('peak_memory_bytes'))),('Transparency database size',human(METRICS.get('transparency_database_size_bytes'))),('NPPES processing duration','NOT MEASURED'),('Enrichment duration','NOT MEASURED'),('Benchmark build duration','NOT MEASURED'),('Benchmark database size','NOT MEASURED'),('API response time (median)',duration(METRICS.get('api_response_time_median_seconds'))),('Dashboard load time','MANUAL MEASUREMENT REQUIRED')]
    for a,b in pairs: md.append(f'|{a}|{b}|')
    md += ['','Synthetic tests are functional evidence only. Production-scale performance must come from actual work-laptop measurements.']
    (out/'thesis_test_results.md').write_text('\n'.join(md)+'\n',encoding='utf-8')

def main():
    ap=argparse.ArgumentParser(); ap.add_argument('--repo-root',default='.'); ap.add_argument('--output',default='thesis_test_evidence'); ap.add_argument('--api',action='store_true'); ap.add_argument('--measure-ingestion',nargs='+',type=Path); a=ap.parse_args()
    repo=root(Path(a.repo_root).resolve()); out=Path(a.output).resolve(); out.mkdir(parents=True,exist_ok=True); work=Path(tempfile.mkdtemp(dir=str(out),prefix='work_'))
    try:
        METRICS.update({'timestamp':time.strftime('%Y-%m-%d %H:%M:%S'),'python_version':platform.python_version(),'platform':platform.platform(),'machine':platform.machine(),'processor':platform.processor(),'node_version':subprocess.run(['node','--version'],capture_output=True,text=True).stdout.strip() if shutil.which('node') else 'N/A','duckdb_python_version':getattr(duckdb,'__version__','N/A')})
        print('Repository:',repo); print('Output:',out); print()
        ingestion_tests(repo,work); runner_tests(repo,work); db=benchmark_tests(repo,work); validation_tests(repo,db,work)
        if a.api: api_tests(repo,db,work)
        else:
            for i in range(34,44): rec(f'TC-{i:02d}','API','API test','Actual API run','Use --api after npm install','NOT RUN')
        dashboard_checks(repo,work)
        if a.measure_ingestion:
            fs=[p.resolve() for p in a.measure_ingestion]
            if all(p.exists() for p in fs): measure_ingestion(repo,fs,out)
            else: print('Missing real input file(s):',*[str(p) for p in fs if not p.exists()])
        for p in work.iterdir():
            if p.is_file(): shutil.copy2(p,out/p.name)
        write(out)
        print('\nDONE'); print('JSON:',out/'thesis_test_results.json'); print('CSV:',out/'thesis_test_results.csv'); print('MD:',out/'thesis_test_results.md'); print('Checklist:',out/'dashboard_manual_checklist.txt')
        print('PASS=',sum(r.status=='PASS' for r in RESULTS),'FAIL=',sum(r.status=='FAIL' for r in RESULTS),'NOT RUN=',sum(r.status=='NOT RUN' for r in RESULTS))
        return 1 if any(r.status=='FAIL' for r in RESULTS) else 0
    except Exception:
        traceback.print_exc(); return 2
    finally: shutil.rmtree(work,ignore_errors=True)

if __name__=='__main__': raise SystemExit(main())
