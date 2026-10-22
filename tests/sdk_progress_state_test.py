"""Check Python/Node progress wire parity without live domain processes."""
import json
import subprocess
import sys
from pathlib import Path
ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / 'sdk/python'))
from atelier_sdk import DomainLogger
class Sink:
    def __init__(self): self.records = []
    def _emit_log(self, record): self.records.append(record)
sink = Sink()
logger = DomainLogger(sink)
cases = [{}, {'done': True}, {'state': 'running', 'done': True},
         *({'state': s, 'waiting': True, 'frac': .4} for s in ['completed', 'failed', 'cancelled'])]
for opts in cases: logger.progress('test', **opts)
try: logger.progress('test', state='invalid')
except ValueError: pass
else: raise AssertionError('Python accepted invalid state')
script = '''const {DomainLogger}=require('./sdk/nodejs');
const records=[]; const logger=new DomainLogger({_emitLog:r=>records.push(r)});
for(const opts of JSON.parse(process.argv[1])) logger.progress('test',opts);
let rejected=false;try{logger.progress('test',{state:'invalid'})}catch(e){rejected=true}
if(!rejected)throw Error('invalid state accepted');
console.log(JSON.stringify(records));'''
node = json.loads(subprocess.check_output(['node', '-e', script, json.dumps(cases)], cwd=ROOT))
for a,b in zip(sink.records,node):
    for key in ['state', 'done', 'waiting', 'frac']:
        assert a[key] == b[key], (key,a,b)
assert [r['state'] for r in node] == ['running','completed','running','completed','failed','cancelled']
assert [r['done'] for r in node] == [False, True, False, True, True, True]
assert all(not r['waiting'] for r in node[3:])
print('Python/Node progress state parity passed')
