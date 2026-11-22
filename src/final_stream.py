"""Kafka replay and inference. The consumer never queries the analytical database."""
from __future__ import annotations
import argparse
from datetime import date, datetime, timezone, timedelta
import hashlib
import json
import math
import os
from pathlib import Path
import sqlite3
import time
import uuid

import joblib
import pandas as pd
import psycopg
from confluent_kafka import Consumer, Producer, KafkaException
from confluent_kafka.admin import AdminClient, NewTopic
from final_ml import FEATURE_COLUMNS, NUMERIC_FEATURES, CITIES, build_features

TOPIC = 'india.aqi.features.v1'
SCHEMA = 'india.aqi.next-day.v1'

def now():
    return datetime.now(timezone.utc).isoformat()

def read_warehouse(database_url):
    with psycopg.connect(database_url) as conn:
        with conn.cursor() as cur:
            cur.execute('''SELECT v.* FROM india_dw.air_quality_enriched v
                JOIN india_dw.etl_runs r ON v.run_id=r.run_id
                WHERE (r.quality_result->>'success')::boolean IS TRUE
                ORDER BY v.date,v.city_id''')
            df = pd.DataFrame(cur.fetchall(), columns=[c.name for c in cur.description])
    if len(df) != 18265 or df[['city_id','date']].duplicated().any():
        raise ValueError('Expected a complete, validated warehouse (18,265 city/day records).')
    return df

def event_from_row(row, replay_id):
    origin, target = str(row['origin_date'])[:10], str(row['target_date'])[:10]
    event_id = str(uuid.uuid5(uuid.NAMESPACE_URL,
        f"india:{replay_id}:{row['run_id']}:{row['city_id']}:{origin}"))
    features = {name: (str(row[name]) if name == 'city_name' else float(row[name])) for name in FEATURE_COLUMNS}
    return {'schema_version':SCHEMA,'event_id':event_id,'published_at':now(),
        'replay_id':replay_id,'simulation':True,'origin_date':origin,'target_date':target,
        'city_id':int(row['city_id']),'source_measurement_id':int(row['measurement_id']),
        'source_run_id':str(row['run_id']),'features':features}

def validate_event(event):
    if not isinstance(event, dict) or event.get('schema_version') != SCHEMA or event.get('simulation') is not True:
        raise ValueError('Unsupported event schema or missing simulation marker')
    required = {'schema_version','event_id','published_at','replay_id','simulation','origin_date',
        'target_date','city_id','source_measurement_id','source_run_id','features'}
    if set(event) != required:
        raise ValueError('Unexpected/missing fields; targets must not be included in an event')
    if not isinstance(event['event_id'], str):
        raise ValueError('event_id must be a UUID string')
    uuid.UUID(event['event_id'])
    if not all(isinstance(event[k], str) and event[k].strip() for k in ['replay_id','source_run_id']):
        raise ValueError('Missing provenance')
    ts = datetime.fromisoformat(event['published_at'])
    if ts.tzinfo is None:
        raise ValueError('published_at must include timezone')
    origin, target = date.fromisoformat(event['origin_date']), date.fromisoformat(event['target_date'])
    if target != origin + timedelta(days=1):
        raise ValueError('Target must be the following local day')
    if not date(2015,1,7) <= origin <= date(2024,12,31):
        raise ValueError('Origin date outside the historical contract')
    for k in ['city_id','source_measurement_id']:
        if type(event[k]) is not int or event[k] < 1:
            raise ValueError('Invalid identifier')
    f = event['features']
    if not isinstance(f,dict) or set(f) != set(FEATURE_COLUMNS) or f['city_name'] not in CITIES:
        raise ValueError('Feature contract mismatch')
    for k in NUMERIC_FEATURES:
        if type(f[k]) not in (int,float) or not math.isfinite(f[k]):
            raise ValueError('Non-finite/non-numeric feature: '+k)
        if k not in ['temperature_lag1'] and f[k] < 0:
            raise ValueError('Negative feature: '+k)
    if f['month'] != target.month or f['day_of_week'] != target.weekday():
        raise ValueError('Calendar features do not match target date')
    if not -90 <= f['temperature_lag1'] <= 60:
        raise ValueError('Temperature outside quality contract')
    return event

def open_store(path):
    Path(path).parent.mkdir(parents=True,exist_ok=True)
    con=sqlite3.connect(path,timeout=30)
    con.execute('PRAGMA journal_mode=WAL')
    con.executescript('''CREATE TABLE IF NOT EXISTS predictions (
        event_id TEXT PRIMARY KEY, replay_id TEXT NOT NULL, city_name TEXT NOT NULL,
        origin_date TEXT NOT NULL, target_date TEXT NOT NULL, predicted_aqi REAL NOT NULL,
        model_version TEXT NOT NULL, processed_at TEXT NOT NULL, published_at TEXT NOT NULL,
        topic TEXT NOT NULL, partition_id INTEGER NOT NULL, kafka_offset INTEGER NOT NULL,
        event_json TEXT NOT NULL);
        CREATE TABLE IF NOT EXISTS rejected_events (
        topic TEXT NOT NULL, partition_id INTEGER NOT NULL, kafka_offset INTEGER NOT NULL,
        rejected_at TEXT NOT NULL, reason TEXT NOT NULL, payload TEXT NOT NULL,
        PRIMARY KEY(topic,partition_id,kafka_offset));''')
    return con

def process_message(raw, model, metadata, con, topic, partition, offset):
    try:
        event=validate_event(json.loads(raw))
    except (ValueError,TypeError,KeyError,OverflowError) as exc:
        with con:
            con.execute('INSERT OR IGNORE INTO rejected_events VALUES (?,?,?,?,?,?)',
                (topic,partition,offset,now(),str(exc),raw.decode('utf-8','replace') if isinstance(raw,bytes) else str(raw)))
        return 'rejected'
    if con.execute('SELECT 1 FROM predictions WHERE event_id=?',(event['event_id'],)).fetchone():
        return 'duplicate'
    prediction=float(model.predict(pd.DataFrame([event['features']],columns=FEATURE_COLUMNS))[0])
    if not math.isfinite(prediction):
        raise RuntimeError('Model returned a non-finite prediction; offset not committed')
    with con:
        con.execute('INSERT OR IGNORE INTO predictions VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?)',
            (event['event_id'],event['replay_id'],event['features']['city_name'],event['origin_date'],
             event['target_date'],prediction,metadata['model_version'],now(),event['published_at'],
             topic,partition,offset,json.dumps(event,ensure_ascii=False)))
    return 'predicted'

def create_topic(bootstrap,topic):
    admin=AdminClient({'bootstrap.servers':bootstrap})
    futures=admin.create_topics([NewTopic(topic,num_partitions=1,replication_factor=1)])
    for future in futures.values():
        try: future.result(30)
        except KafkaException as exc:
            from confluent_kafka import KafkaError
            if exc.args[0].code()!=KafkaError.TOPIC_ALREADY_EXISTS: raise

def produce(database_url,bootstrap,topic,start,end,delay,limit,replay_id):
    if delay<0 or limit<0: raise ValueError('delay/limit must be nonnegative')
    features=build_features(read_warehouse(database_url))
    features=features[(features.target_date>=start)&(features.target_date<=end)]
    if limit: features=features.head(limit)
    if features.empty: raise ValueError('No events in the selected date range')
    producer=Producer({'bootstrap.servers':bootstrap,'enable.idempotence':True,'acks':'all',
        'delivery.timeout.ms':30000,'client.id':'india-replay-producer'})
    errors=[]
    def delivered(error,message):
        if error: errors.append(str(error))
    count=0
    for row in features.to_dict('records'):
        event=validate_event(event_from_row(row,replay_id))
        producer.produce(topic,key=str(row['city_id']),value=json.dumps(event).encode(),on_delivery=delivered)
        producer.poll(0)
        count+=1
        if errors: raise RuntimeError(errors[0])
        if delay: time.sleep(delay)
    pending=producer.flush(35)
    if pending or errors: raise RuntimeError(f'Kafka delivery failed: {pending}, {errors}')
    summary={'sent':count,'replay_id':replay_id,'topic':topic,'simulation':True,'completed_at':now()}
    print(json.dumps(summary));return summary

def consume(bootstrap,topic,group,artifacts,store_path,max_messages=0,idle_timeout=0):
    artifacts=Path(artifacts)
    metadata=json.loads((artifacts/'metadata.json').read_text(encoding='utf-8'))
    if metadata['feature_columns']!=FEATURE_COLUMNS:
        raise ValueError('Model and consumer feature contracts differ')
    if hashlib.sha256((artifacts/'model.joblib').read_bytes()).hexdigest()!=metadata['model_sha256']:
        raise ValueError('Model artifact hash differs from training metadata')
    # Only load the locally trained trusted artifact, once at process startup.
    model=joblib.load(artifacts/'model.joblib')
    con=open_store(store_path)
    consumer=Consumer({'bootstrap.servers':bootstrap,'group.id':group,'auto.offset.reset':'earliest',
        'enable.auto.commit':False,'client.id':'india-inference-consumer'})
    counts={'predicted':0,'duplicate':0,'rejected':0}
    started=last=time.monotonic()
    consumer.subscribe([topic])
    try:
        while True:
            msg=consumer.poll(1)
            if msg is None:
                if idle_timeout and time.monotonic()-last>idle_timeout: break
                continue
            if msg.error(): raise KafkaException(msg.error())
            status=process_message(msg.value(),model,metadata,con,msg.topic(),msg.partition(),msg.offset())
            # Commit only after durable prediction/rejection storage.
            consumer.commit(message=msg,asynchronous=False)
            counts[status]+=1;last=time.monotonic()
            print(json.dumps({'status':status,'offset':msg.offset(),'counts':counts}),flush=True)
            if max_messages and sum(counts.values())>=max_messages: break
    finally:
        consumer.close();con.close()
    result={**counts,'topic':topic,'group':group,'model_version':metadata['model_version'],
        'duration_seconds':round(time.monotonic()-started,2),'completed_at':now()}
    print(json.dumps(result));return result

def main():
    parser=argparse.ArgumentParser(description=__doc__)
    parser.add_argument('action',choices=['create-topic','produce','consume'])
    parser.add_argument('--bootstrap',default=os.getenv('KAFKA_BOOTSTRAP_SERVERS','localhost:19092'))
    parser.add_argument('--topic',default=os.getenv('KAFKA_TOPIC',TOPIC))
    parser.add_argument('--database-url',default=os.getenv('INDIA_DATABASE_URL'))
    parser.add_argument('--start',default='2023-01-01');parser.add_argument('--end',default='2024-12-31')
    parser.add_argument('--delay',type=float,default=.5);parser.add_argument('--limit',type=int,default=0)
    parser.add_argument('--replay-id',default=str(uuid.uuid4()))
    parser.add_argument('--group',default='india-final-inference-v1')
    parser.add_argument('--artifacts',default='artifacts');parser.add_argument('--store',default='runtime/predictions.sqlite')
    parser.add_argument('--max-messages',type=int,default=0);parser.add_argument('--idle-timeout',type=float,default=0)
    args=parser.parse_args()
    if args.action=='create-topic':create_topic(args.bootstrap,args.topic)
    elif args.action=='produce':produce(args.database_url,args.bootstrap,args.topic,args.start,args.end,args.delay,args.limit,args.replay_id)
    else:consume(args.bootstrap,args.topic,args.group,args.artifacts,args.store,args.max_messages,args.idle_timeout)

if __name__=='__main__':main()
