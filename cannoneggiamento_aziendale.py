#!/usr/bin/python3
# -*- coding: utf-8 -*-

"""
    Questo script è lo slave del cannone.
    Viene chiamato il metodo main dal master che è lo scirpt edi_cannon_master.
"""

import io
import logging
import select
import sys
import time
import traceback
import psycopg2
import psycopg2.extras
from psycopg2.extras import Json
import argo_data_retriever
import internauta_data_manager as idm
from logging.handlers import TimedRotatingFileHandler
from db_connection import DB_INTERNAUTI, db_minirepo

log = logging.getLogger("cannoneggiamento_aziendale")

def try_lock(conn):
    q = "select pg_try_advisory_lock(2243247306)"
    c = conn.cursor(cursor_factory=psycopg2.extras.DictCursor)
    c.execute(q)
    return c.fetchone()[0]


def take_lock(conn):
    q = "select pg_advisory_lock(2243247306)"
    c = conn.cursor(cursor_factory=psycopg2.extras.DictCursor)
    c.execute(q)
    return c.fetchone()[0]

def unlock(conn):
    q = "select pg_advisory_unlock(2243247306)"
    c = conn.cursor(cursor_factory=psycopg2.extras.DictCursor)
    c.execute(q)
    return c.fetchone()[0]


def got_delete_too(row, rows):
    got_to_delete_the_row = False;
    for r in rows:
        if(row['id_oggetto'] == r['id_oggetto'] and row['tipo_oggetto'] == r['tipo_oggetto'] and r['operazione'] == "DELETE"):
            got_to_delete_the_row = True
            break
    return got_to_delete_the_row


def set_guids_in_esecuzione(row, conn):
    #log = logging.getLogger("cannoneggiamento_aziendale")
    qUpdate = """ update esportazioni.cannoneggiamenti
    set in_esecuzione = true
    where id in %s """
    try:
        c = conn.cursor(cursor_factory=psycopg2.extras.DictCursor)
        c.execute(qUpdate, (tuple(i for i in row['ids']),))
        conn.commit()
        log.info("set_guids_in_esecuzione eseguita con successo")
    except Exception as ex:
        log.error("errore nel set_guids_in_esecuzione")
        log.error(c.query)
        raise ex


def set_guids_in_error(row, conn, codice_azienda, ex):
    #log = logging.getLogger("cannoneggiamento_aziendale")
    log.info('setto guid in errore')
    qError = """ update esportazioni.cannoneggiamenti
    set in_error = true
    where id in %s """
    c = conn.cursor(cursor_factory=psycopg2.extras.DictCursor)
    log.error(qError % str(tuple(i for i in row[3])))
    c.execute(qError, (tuple(i for i in row[3]),))
    conn.commit()
    erroro(conn, codice_azienda, ex)


def delete_cannoneggiamenti(ids, conn):
    delete = "DELETE FROM esportazioni.cannoneggiamenti WHERE id = ANY(%(ids)s)"
    try:
        c = conn.cursor(cursor_factory=psycopg2.extras.DictCursor)
        c.execute(delete, {
            "ids": ids
        })
        conn.commit()
    except Exception as ex:
        log.error("delete_cannoneggiamenti fallita")
        log.error(c.query)
        raise ex


def erroro(conn, codice_azienda, ex):
    c = conn.cursor(cursor_factory=psycopg2.extras.DictCursor)
    qs = """select (count(id) = 0) as non_esiste 
            from diagnostica.report 
            where tipologia ='CANNONEGGIAMENTO_ERROR' and 
                risolto = false and 
                in_attesa_di_risoluzione = false
        """
    c.execute(qs, {'codice_azienda': codice_azienda})
    r = c.fetchone()
    #non so perche non prenda la mappa porca mignotta
    if r['non_esiste']:
        # inserisco
        qi = """INSERT INTO diagnostica.report
                    (tipologia, data_inserimento_riga, additional_data, risolto, in_attesa_di_risoluzione)
                VALUES('CANNONEGGIAMENTO_ERROR', now(), %(additional_data)s, false, false)
            """
        c.execute(qi, {'additional_data': Json('{"Exception": "'+str(ex)+'", "codice_azienda":"'+str(codice_azienda)+'"')})
        conn.commit()
    else:
        # non faccio nulla
        pass





def get_internauta_conn():
    #log = logging.getLogger("cannoneggiamento_aziendale")
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


def get_minirepo_conn():
    log.info("mi connetto a minirepo")
    try:
        minio_conn = psycopg2.connect(
            user=db_minirepo['user'],
            password=db_minirepo['password'],
            host=db_minirepo['host'],
            database=db_minirepo['database']
        )
        return minio_conn
    except Exception as ex:
        log.error("Non sono riuscito a connettermi ad internauta")
        log.error(ex)

"""
    Guardo quali sono i cannoneggiamenti richiesti e li eseguo
"""
def search_and_work(conn, codice_azienda, fascicoli_parlanti, conn_internauta, id_azienda):
    log.info("Eseguo search_and_work....")
    select_cannoneggiamenti = """
        WITH id_presi_in_carico AS (
            UPDATE esportazioni.cannoneggiamenti
            SET in_esecuzione = TRUE 
            WHERE NOT in_esecuzione
            AND NOT in_error
            RETURNING id, id_oggetto, tipo_oggetto, operazione
        )
        SELECT id_oggetto, tipo_oggetto, array_agg(operazione) AS operazioni, array_agg(id) as ids
        FROM id_presi_in_carico
        GROUP BY id_oggetto, tipo_oggetto
    """
    try:
        c = conn.cursor(cursor_factory=psycopg2.extras.DictCursor)
        c.execute(select_cannoneggiamenti)
        rows = c.fetchall()
        for r in rows:
            try:
                log.info("Tipologia %s" % r['tipo_oggetto'])
                if "DELETE" in r["operazioni"]:
                    # Devo cancellare questo oggetto (è previsto che sia un pico/dete/deli), lo cancello e poi posso eliminare tutte le righe corrispondenti
                    log.info("Delete del guid: %s, tipo: %s, azienda: %s" % (r['id_oggetto'], r["tipo_oggetto"], codice_azienda))
                    now = time.time()
                    idm.delete_doc_by_guid_and_azienda(r['id_oggetto'], conn_internauta, id_azienda)
                    later = time.time()
                    difference = int(later - now)
                    log.info("Delete eseguita con successo in %s secondi" % str(difference))
                else:
                    # Gestisco update/insert in modo differente a seconda del tipo oggetto su cui agire
                    if r['tipo_oggetto'] in ["pico_pe", "pico_pu", "dete", "deli"]:
                        json_data = argo_data_retriever.get_document_by_guid(conn, r['id_oggetto'], r['tipo_oggetto'])
                        # Se il doc esiste dovrei avere dei dati, se è così vado a fare la upsert
                        if json_data is not None:
                            idm.upsert_doc_list_data(codice_azienda, json_data, conn_internauta, id_azienda)
                    elif r['tipo_oggetto'] == "fascicolo":
                        # Se l'oggetto è il fascicolo allora si tratta dell'update del nome
                        nome = "" if fascicoli_parlanti else argo_data_retriever.get_nome_fascicolo(conn, r['id_oggetto'])
                        idm.update_nome_fascicoli(nome, r['id_oggetto'], conn_internauta)
                # Se sono arrivato fin qui ho eseugito l'azione opportuna sull'oggetto, cancello le righe di cannoneggiamento
                delete_cannoneggiamenti(r["ids"], conn)
            except Exception as ex:
                log.error("ERRORE! SEARCH_AND_WORK")
                log.error(ex)
                errore = ex.args[0]
                set_guids_in_error(r, conn, codice_azienda, errore)
    except Exception as ex:
        log.error("SEARCH_AND_WORK errore nel reperimento delle righe o del parametro nome parlante")
        log.error(c.query)
        log.error(ex)
        raise ex


def setta_log(azienda):
    filename = "log/edi_cannon_" + str(azienda) + ".log"
    fmt = logging.Formatter('%(asctime)s %(message)s', datefmt='%Y-%m-%d %I:%M:%S %p')
    hnd = TimedRotatingFileHandler(filename, when='midnight', interval=1, backupCount=7)
    hnd.setFormatter(fmt)
    log.addHandler(hnd)
    log.setLevel(logging.INFO)

"""
    Tipico main che si connette a db e si mette in ascolto di una notify.
    Quando scatta una notify chiama: search_and_work
"""
def main(codice_azienda, host, user, password, db):
    setta_log(codice_azienda)
    while True:
        try:
            conn = psycopg2.connect(
                user=user,
                password=password,
                host=host,
                database=db
            )

            if not try_lock(conn):
                log.info("Un'altra istanza dello script e' in esecuzione. Esco.")
                sys.exit(0)
            else:
                take_lock(conn)

            # Autocommit obbligatorio per stare in ascolto di notify.
            conn.set_isolation_level(psycopg2.extensions.ISOLATION_LEVEL_AUTOCOMMIT)
            log.info("Guardo se c'è del lavoro da fare subito")

            curs = conn.cursor(cursor_factory=psycopg2.extras.DictCursor)
            
            # Devo ancora fare il primo giro, se era rimasta roba in esecuzione ma non in errore la setto da fare
            # La roba in errore la lascio in errore
            curs.execute("""
                UPDATE esportazioni.cannoneggiamenti
	            SET in_esecuzione = false
                WHERE in_esecuzione 
                AND not in_error
            """)

            # L'azienda è con fascioli parlanti?
            curs.execute("SELECT val_parametro::int <> 0 FROM bds_tools.parametri_pubblici pp WHERE pp.nome_parametro = 'fascicoliParlanti'")
            fascicoli_parlanti = curs.fetchone()[0]

            conn_internauta = get_internauta_conn()
            cursor_internauta = conn_internauta.cursor(cursor_factory=psycopg2.extras.DictCursor)
            cursor_internauta.execute("SELECT id FROM baborg.aziende WHERE codice = %(codice_azienda)s", {
                "codice_azienda": codice_azienda
            })
            id_azienda = cursor_internauta.fetchone()["id"]
            cursor_internauta.close()

            # Magari c'è dell'arretrato, controllo e lo faccio
            search_and_work(conn, codice_azienda, fascicoli_parlanti, conn_internauta, id_azienda)

            curs.execute("LISTEN cannoneggiamenti_argo;")
            log.info("In ascolto per altro lavoro..")

            while 1:
                if select.select([conn], [], []):
                    log.info("Ricevuta notifica di altro lavoro")
                    conn.poll()
                    while conn.notifies:
                        del conn.notifies[:]
                        search_and_work(conn, codice_azienda, fascicoli_parlanti, conn_internauta, id_azienda)
        except:
            log.error("Probabile errore nella connessione al db o nell'esecuzione del lavoro.")
            output = io.StringIO()
            traceback.print_exception(*sys.exc_info(), limit=None, file=output)
            log.error(output.getvalue())
            time.sleep(10)
        finally:
            try:
                unlock(conn)
            except:
                pass