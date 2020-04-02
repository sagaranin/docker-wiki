import json
from sseclient import SSEClient as EventSource
from sqlalchemy import create_engine, Column, String, DateTime, func
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.dialects import postgresql
from sqlalchemy.orm import sessionmaker
from sys import getsizeof

engine = create_engine('postgres://postgres:Ralf%4012358@db/wiki', echo=True)
conn = engine.connect()
Session = sessionmaker(bind=engine)
session = Session()
Base = declarative_base()


class Event(Base):
    __tablename__ = 'events_raw'

    meta_id = Column(String, primary_key=True)
    meta_dt = Column(DateTime)
    data = Column(postgresql.JSONB)


Base.metadata.create_all(engine)
buffer = []
cache = set()


# fill cache
print("Filling cache...")
rs = conn.execution_options(stream_results=True)\
        .execute(f'select meta_id from {Event.__tablename__} order by meta_dt desc limit 1000000')
while True:
    batch = rs.fetchmany(10000)
    if not batch:
        break
    for row in batch:
        cache.add(row.meta_id)

rs.close()
print(f"Initial cache size is: {len(cache)}, in memory: {getsizeof(cache)/1024/1024} MBytes")

# get last date
max_date = session.query(func.max(Event.meta_dt)).scalar()
date_from = max_date.strftime('%Y-%m-%dT%H:%M:%SZ')
print(f"Reload from date: {date_from}")


url = f'https://stream.wikimedia.org/v2/stream/recentchange?since={date_from}'
print(f"Using SSE URL {url}")
for event in EventSource(url, retry=1000, chunk_size=81920000):
    try:
        change = json.loads(event.data)
    except ValueError as ex:
        print(ex, event.data)
    else:
        if change['meta']['id'] not in cache:
            buffer.append(Event(meta_id=change['meta']['id'], meta_dt=change['meta']['dt'], data=change))
            cache.add(change['meta']['id'])
        else:
            print(f"ID {change['meta']['id']} {change['meta']['dt']} already in cache")

        if len(buffer) >= 5000:
            session.bulk_save_objects(buffer)
            session.commit()
            print(f"New cache size is: {len(cache)},  in memory: {getsizeof(cache)/1024/1024} MBytes")
            buffer.clear()
