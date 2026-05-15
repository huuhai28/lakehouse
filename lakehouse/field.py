from unidecode import unidecode
import json, re, logging
from kafka import KafkaConsumer, KafkaProducer
from kafka.admin import KafkaAdminClient, NewTopic
from kafka.errors import TopicAlreadyExistsError

logging.basicConfig(level=logging.INFO, format='%(asctime)s %(levelname)s %(message)s')
log = logging.getLogger(__name__)
BOOTSTRAP = 'localhost:9092'
GROUP_ID = 'field-sanitizer-v3'

def sanitize(name):
    if not name: return name
    res = unidecode(name)
    res = re.sub(r'[^a-zA-Z0-9_]', '_', res)
    res = re.sub(r'_+', '_', res).strip('_')
    if res and res[0].isdigit():
        res = 'col_' + res
    return res.lower() if res else 'col'

def transform_message(v, t):
    if not v: return v
    try:
        m = json.loads(v.decode('utf-8'))
        s = m.get('schema', {})
        p = m.get('payload', {})
        parts = t.split('.')
        db_name = parts[0][len('topic_'):]
        tbl = parts[2]
        target_table = f"db_{db_name.lower()}.{tbl}"

        f_map = {}
        n_fields = []
        for f in s.get('fields', []):
            if f.get('field') in ('after', 'before') and f.get('type') == 'struct':
                n_struct_fields = []
                for sf in f.get('fields', []):
                    old_n = sf.get('field')
                    new_n = sanitize(old_n)
                    f_map[old_n] = new_n
                    nf = dict(sf)
                    nf['field'] = new_n
                    n_struct_fields.append(nf)
                nf = dict(f)
                nf['fields'] = n_struct_fields
                n_fields.append(nf)
            else:
                n_fields.append(f)

        # Thêm target_table vào TOP-LEVEL schema
        n_fields.append({"type": "string", "optional": True, "field": "target_table"})

        n_s = dict(s)
        n_s['fields'] = n_fields

        n_p = dict(p)
        for st in ['after', 'before']:
            if st in p and p[st]:
                n_p[st] = {f_map.get(k, k): val for k, val in p[st].items()}

        # Thêm target_table vào TOP-LEVEL payload
        n_p['target_table'] = target_table

        return json.dumps({'schema': n_s, 'payload': n_p}, ensure_ascii=False).encode('utf-8')
    except Exception as e:
        log.error(f"Transform error: {e}")
        return v

def ensure_topic(admin, topic_name, created_cache):
    if topic_name in created_cache:
        return
    try:
        admin.create_topics([NewTopic(name=topic_name, num_partitions=1, replication_factor=1)])
    except TopicAlreadyExistsError:
        pass
    except Exception as e:
        log.warning(f"Topic create warning {topic_name}: {e}")
    created_cache.add(topic_name)

def main():
    # Tăng max_poll_interval_ms để tránh rebalance khi xử lý chậm
    # Giảm max_poll_records để poll nhanh hơn
    consumer = KafkaConsumer(
        bootstrap_servers=BOOTSTRAP,
        group_id=GROUP_ID,
        auto_offset_reset='earliest',
        max_poll_records=50,           # Giảm từ default 500 → tránh quá tải mỗi poll
        max_poll_interval_ms=600000,   # 10 phút — tránh rebalance khi xử lý nhiều
        session_timeout_ms=45000,
        heartbeat_interval_ms=15000,
        enable_auto_commit=True,
        auto_commit_interval_ms=5000,
    )
    producer = KafkaProducer(
        bootstrap_servers=BOOTSTRAP,
        linger_ms=100,           # Batch nhỏ để giảm latency
        batch_size=65536,        # 64KB per batch
        acks=1,                  # Không chờ full ack để tăng tốc
        retries=3,
        # BỎ compression_type='lz4' để không bị lỗi AssertionError
    )
    admin = KafkaAdminClient(bootstrap_servers=BOOTSTRAP)
    consumer.subscribe(pattern=re.compile(r'^topic_[^.]+\.[^.]+\.[^.]+$'))
    log.info("STARTING field-sanitizer v3 (FIXED LZ4)")

    created_topics = set()
    count = 0

    for msg in consumer:
        try:
            parts = msg.topic.split('.')
            if len(parts) < 3:
                continue
            db = parts[0][len('topic_'):]
            tbl = parts[2]
            dt = f"iceberg_v9.{db}.{tbl}"

            ensure_topic(admin, dt, created_topics)

            out = transform_message(msg.value, msg.topic)
            # Gửi bất đồng bộ — không block để tránh vượt max_poll_interval
            producer.send(dt, value=out)
            count += 1

            if count % 500 == 0:
                producer.flush()  # Flush định kỳ mỗi 500 messages
                log.info(f"Processed {count} messages")

        except Exception as e:
            log.error(f"Main error on {msg.topic}: {e}")

if __name__ == '__main__':
    main()
