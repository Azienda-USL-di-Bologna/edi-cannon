import io
import select
import sys
import time
import traceback
import logging
from cannoneggiamento_aziendale import CannoneggiamentoAziendale
import psycopg2

from db_connection import DB_CONFIG as db
import internauta_data_manager as idr



logging.basicConfig(
    filename='edi_cannon.log',
    level=logging.INFO,
    format='%(asctime)s %(message)s',
    datefmt='%Y-%m-%d %I:%M:%S %p'
)
log = logging.getLogger("edi_cannon")


def take_lock(conn):
    q = "select pg_advisory_lock(2243247306)"
    c = conn.cursor()
    c.execute(q)
    return c.fetchone()[0]


def try_lock(conn):
    q = "select pg_try_advisory_lock(2243247306)"
    c = conn.cursor()
    c.execute(q)
    return c.fetchone()[0]


def get_aziende_avec():
    return idr.get_aziende();


if __name__ == '__main__':
    try:
        conn = psycopg2.connect(

            user=db['internauta']['user'],
            password=db['internauta']['password'],
            host=db['internauta']['host'],
            database=db['internauta']['db']
        )
        log.info("Connessione al db stabilita")

        if not try_lock(conn):
            log.info("Un'altra istanza dello script e' in esecuzione. Esco.")
            sys.exit(0)
        else:
            take_lock(conn)

        # Autocommit obbligatorio per stare in ascolto di notify.
        conn.set_isolation_level(psycopg2.extensions.ISOLATION_LEVEL_AUTOCOMMIT)

        log.info("Recupero le aziende...")
        res = get_aziende_avec()
        for a in res:
            print(str(a))
            if a[0] == 2:
                az = CannoneggiamentoAziendale(str(a[0]), str(a[1]), str(a[2]))
                log.info(str(a))
                az.run()

    except:
        log.error("Probabile errore nella connessione al db o nell'esecuzione del lavoro.")
        output = io.StringIO()
        traceback.print_exception(*sys.exc_info(), limit=None, file=output)
        log.error(output.getvalue())
        if conn:
            conn.close()
        time.sleep(10)