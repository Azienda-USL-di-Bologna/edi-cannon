import psycopg2
import psycopg2.extras
import hashlib


def try_lock_all_guid(conn, guid, tipo_oggetto):
    #ritorna true se ha preso il lock
    c = conn.cursor(cursor_factory=psycopg2.extras.DictCursor)
    for_hash = tipo_oggetto + guid
    n_for_lock = int(hashlib.sha256(for_hash.encode("utf-8")).hexdigest(), 16) % (10 ** 9)
    q = "select pg_try_advisory_lock(2243247::integer, %(n_for_lock)s)"
    c.execute(q, {'n_for_lock': n_for_lock})
    return c.fetchone()[0]


def unlock_all_guid(conn, guid, tipo_oggetto):
    c = conn.cursor(cursor_factory=psycopg2.extras.DictCursor)
    for_hash = tipo_oggetto + guid
    n_for_lock = int(hashlib.sha256(for_hash.encode("utf-8")).hexdigest(), 16) % (10 ** 9)
    q = "select pg_advisory_unlock(2243247::integer,%(n_for_lock)s)"
    c.execute(q, {'n_for_lock': n_for_lock})
    return c.fetchone()[0]


def delete_all_guid_done(conn, guid):
    pass


def connetti(db):
    conn = psycopg2.connect(
        user=db['user'],
        password=db['password'],
        host=db['host'],
        database=db['database']
    )
    return conn


def get_numero_thread_and_set_todo(db):
    conn = connetti(db)
    c = conn.cursor(cursor_factory=psycopg2.extras.DictCursor)
    c.execute("select val_parametro from bds_tools.parametri_pubblici where nome_parametro = 'nProcessEdiCannon'")
    n_thread = c.fetchone()[0]
    print('recupero il numero di processi da instanziare %s', str(n_thread))
    print('pulisco la tabella per cannoneggiare lo stretto indispensabile')
    c.execute('''DELETE FROM esportazioni.cannoneggiamenti 
                WHERE id IN
                    (SELECT id
                    FROM 
                        (SELECT id,
                         ROW_NUMBER() OVER( PARTITION BY id_oggetto, tipo_oggetto, operazione
                        ORDER BY  id ) AS row_num
                        FROM  esportazioni.cannoneggiamenti   ) t
                        WHERE t.row_num > 1 )''')
    print('setto in esecuzione false per le esportazioni')
    c.execute("update esportazioni.cannoneggiamenti set in_esecuzione = false where in_esecuzione")
    conn.close()
    return n_thread


def get_internauta_conn(log, DB_INTERNAUTI):
    # log = logging.getLogger("cannoneggiamento_aziendale")
    log.info("mi connetto a internauta")
    try:
        conn = psycopg2.connect(
            user=DB_INTERNAUTI['user'],
            password=DB_INTERNAUTI['password'],
            host=DB_INTERNAUTI['host'],
            database=DB_INTERNAUTI['database']
        )
        return conn
    except Exception as ex:
        log.error("Non sono riuscito a connettermi ad internauta")
        log.error(ex)
