"""Read only the prediction store populated by the separate Kafka consumer."""
import json
import os
from pathlib import Path
import sqlite3
import pandas as pd
import streamlit as st

st.set_page_config(page_title='India | AQI prediction stream',page_icon='🌏',layout='wide')
st.title('India · AQI prediction stream')
st.caption('Historical replay · Five cities · One-day-ahead AQI · Kafka → inference → prediction store')
st.info('Academic simulation using 2015–2024 records. These predictions are not current observations or health guidance.')
store=Path(os.getenv('PREDICTION_STORE','runtime/predictions.sqlite'))
artifacts=Path(os.getenv('MODEL_ARTIFACTS','artifacts'))
if (artifacts/'metadata.json').exists():
    meta=json.loads((artifacts/'metadata.json').read_text(encoding='utf-8'))
    with st.expander('Model and evaluation'):
        st.write('Model version:',meta.get('model_version'))
        st.json(meta.get('metrics',{}))
        st.write('Inputs are historical measurements and weather available by the end of the origin day. The predicted day is the following day.')
        st.warning('A working streaming system does not establish predictive usefulness. Compare model errors with the baseline in the static report.')
city=st.selectbox('City',['All','Bangalore','Chennai','Delhi','Kolkata','Mumbai'])

@st.fragment(run_every='2s')
def live():
    if not store.exists():
        st.warning('Waiting for the Kafka consumer. Start the consumer, then the producer.');return
    with sqlite3.connect(store.resolve().as_uri()+'?mode=ro',uri=True,timeout=5) as conn:
        total=conn.execute('SELECT COUNT(*) FROM predictions').fetchone()[0]
        rejected=conn.execute('SELECT COUNT(*) FROM rejected_events').fetchone()[0]
        rows=pd.read_sql_query('SELECT event_id,city_name,origin_date,target_date,predicted_aqi,model_version,processed_at,topic,partition_id,kafka_offset FROM predictions ORDER BY processed_at DESC LIMIT 1000',conn)
    a,b,c=st.columns(3)
    a.metric('Predictions stored',f'{total:,}')
    b.metric('Messages rejected',rejected)
    c.metric('Last processed (UTC)',rows.processed_at.iloc[0][11:19] if len(rows) else 'Waiting')
    if rows.empty:
        st.write('Connected to the prediction store; no messages processed yet.');return
    view=rows if city=='All' else rows[rows.city_name==city]
    if view.empty:st.write('No predictions for this city yet.');return
    view=view.sort_values(['target_date','processed_at'])
    chart=view.drop_duplicates(['target_date','city_name'],keep='last').pivot(index='target_date',columns='city_name',values='predicted_aqi')
    st.subheader('Predicted AQI by historical target date')
    st.line_chart(chart,x_label='Target date (India local day)',y_label='Predicted AQI')
    st.subheader('Latest Kafka predictions')
    st.dataframe(view.sort_values('processed_at',ascending=False),hide_index=True,width='stretch')
    st.caption('This page reads only consumer output. Kafka offsets, model version and processing time provide message provenance. Last 1,000 predictions shown; counters cover the full store.')

live()
