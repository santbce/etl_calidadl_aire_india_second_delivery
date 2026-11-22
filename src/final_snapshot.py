"""Revalidate a restored second-delivery snapshot without claiming fresh extraction.

For an offline demo, restore reports/warehouse.sql into a NEW database first.
The full Airflow DAG remains the fresh-extraction path for all three sources.
"""
import argparse
import json
import os
from pathlib import Path
import psycopg
import pandas as pd
from pipeline_india import ROOT,workspace,prepare_run,validate_enriched,load_warehouse,utcnow

def revalidate(run_id):
    prepare_run(run_id)
    with psycopg.connect(os.environ['INDIA_DATABASE_URL']) as con:
        with con.cursor() as cur:
            cur.execute('SELECT * FROM india_dw.air_quality_enriched ORDER BY measurement_id')
            df=pd.DataFrame(cur.fetchall(),columns=[d.name for d in cur.description])
    df['run_id']=run_id
    df['merged_at_utc']=utcnow()
    path=workspace(run_id)
    df.to_csv(path/'enriched.csv',index=False)
    (path/'snapshot_provenance.json').write_text(json.dumps({'mode':'restored_second_delivery_snapshot',
        'source_file':'reports/warehouse.sql','fresh_http_extraction':False,'revalidated_at':utcnow()},indent=2))
    validate_enriched(run_id)
    load_warehouse(run_id)
    print('Snapshot revalidated; no new Wikipedia/Open-Meteo extraction was performed.')

if __name__=='__main__':
    parser=argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--run-id',default='snapshot_revalidation')
    args=parser.parse_args()
    from final_setup import prepare
    prepare();revalidate(args.run_id)
