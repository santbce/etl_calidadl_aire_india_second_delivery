"""Unpack the preserved first delivery for the original ETL."""
from pathlib import Path
import os
import zipfile

def prepare():
    root=Path(os.getenv('INDIA_PROJECT_ROOT',Path(__file__).resolve().parents[1]))
    target=root/'primera_entrega'
    if (target/'database/air_quality.db').exists():return
    with zipfile.ZipFile(root/'data/etl-calidad-aire-india-main.zip') as z:
        dbs=[p for p in z.namelist() if p.endswith('/database/air_quality.db')]
        if len(dbs)!=1:raise ValueError('First-delivery archive structure changed')
        prefix=dbs[0].split('database/air_quality.db')[0]
        for name in z.namelist():
            if name.startswith(prefix) and not name.endswith('/'):
                relative=Path(name[len(prefix):])
                dest=(target/relative).resolve()
                if not dest.is_relative_to(target.resolve()):raise ValueError('Unsafe archive member')
                dest.parent.mkdir(parents=True,exist_ok=True)
                dest.write_bytes(z.read(name))
    print('Preserved first-delivery database prepared')
if __name__=='__main__':prepare()
