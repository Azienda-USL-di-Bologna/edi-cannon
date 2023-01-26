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
import os

import utils
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
    got_to_delete_the_row = False
    for r in rows:
        if (row['id_oggetto'] == r['id_oggetto'] and row['tipo_oggetto'] == r['tipo_oggetto'] and r['operazione'] == "DELETE"):
            got_to_delete_the_row = True
            break
    return got_to_delete_the_row


def set_guids_in_esecuzione(row, conn):
    # log = logging.getLogger("cannoneggiamento_aziendale")
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


def set_guids_in_error(row, conn, codice_azienda, ex, guid):
    # log = logging.getLogger("cannoneggiamento_aziendale")
    log.info('setto guid in errore: ' + guid)
    q_error = """ update esportazioni.cannoneggiamenti
    set in_error = true
    where id = ANY(%(ids)s) """
    c = conn.cursor(cursor_factory=psycopg2.extras.DictCursor)
    #log.error(qError % str(tuple(i for i in row[3])))
    c.execute(q_error, {'ids': row['ids']})
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
    # non so perche non prenda la mappa porca mignotta
    if r['non_esiste']:
        # inserisco
        qi = """INSERT INTO diagnostica.report
                    (tipologia, data_inserimento_riga, additional_data, risolto, in_attesa_di_risoluzione)
                VALUES('CANNONEGGIAMENTO_ERROR', now(), %(additional_data)s, false, false)
            """
        c.execute(qi, {
            'additional_data': Json('{"Exception": "' + str(ex) + '", "codice_azienda":"' + str(codice_azienda) + '"')})
        conn.commit()
    else:
        # non faccio nulla
        pass


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
    curs = conn.cursor(cursor_factory=psycopg2.extras.DictCursor)
    try:
        select_cannoneggiamenti = '''
            SELECT id_oggetto, tipo_oggetto, array_agg(operazione) AS operazioni, array_agg(id) as ids
            FROM esportazioni.cannoneggiamenti e
            WHERE not e.in_esecuzione
            AND not in_error
            GROUP BY id_oggetto, tipo_oggetto
            LIMIT 1 
            OFFSET %(offset)s
        '''
        offset = 0
        curs.execute(select_cannoneggiamenti, {'offset': offset})
        log.info('1')
        while curs.rowcount == 1:

            log.info('1.5')
            r = curs.fetchone()
            log.info('2')
            if utils.try_lock_all_guid(conn, r['id_oggetto'], r['tipo_oggetto']):
                log.info('3')
                try:
                    curs.execute("""
                        UPDATE esportazioni.cannoneggiamenti
                        SET in_esecuzione = TRUE 
                        WHERE 
                        id = ANY(%(ids)s)
                    """, {'ids': r['ids']})
                    
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
                        if r['tipo_oggetto'] in ["pico_pe", "pico_pu", "dete", "deli", "RGPICO", "RGDETE", "RGDELI"]:
                            json_data = argo_data_retriever.get_document_by_guid(conn, r['id_oggetto'], r['tipo_oggetto'])
                            # Se il doc esiste dovrei avere dei dati, se è così vado a fare la upsert
                            if json_data is not None:
                                idm.upsert_doc_list_data(codice_azienda, json_data, conn_internauta, id_azienda)
                        elif r['tipo_oggetto'] == "fascicolo":
                            # Se l'oggetto è il fascicolo allora si tratta dell'update del nome
                            # nome = "" if fascicoli_parlanti else argo_data_retriever.get_nome_fascicolo(conn, r['id_oggetto'])
                            # idm.update_nome_fascicoli(nome, r['id_oggetto'], conn_internauta)
                            pass
                    # Se sono arrivato fin qui ho eseugito l'azione opportuna sull'oggetto, cancello le righe di cannoneggiamento
                    # e slocko il documento
                    delete_cannoneggiamenti(r["ids"], conn)
                    utils.unlock_all_guid(conn, r['id_oggetto'], r['tipo_oggetto'])

                except Exception as ex:
                    log.error("ERRORE NEL CANNONNEGGIAMENTO DEL DOCUMENTO")
                    output = io.StringIO()
                    traceback.print_exception(*sys.exc_info(), limit=None, file=output)
                    log.error(output.getvalue())
                    errore = ex.args[0]
                    set_guids_in_error(r, conn, codice_azienda, errore, r['id_oggetto'])

            offset += 1
            curs.execute(select_cannoneggiamenti, {'offset': offset})

    except Exception as ex:
        log.error("SEARCH_AND_WORK errore nel reperimento delle righe o del parametro nome parlante")
        log.error(curs.query)
        log.error(ex)
        raise ex


def setta_log(azienda):
    filename = "log/edi_cannon_" + str(azienda) + ".log"
    if not os.path.exists(filename):
        open(filename, "w").close()
    fmt = logging.Formatter('%(asctime)s %(processName)s %(process)d %(name)s %(levelname)-8s %(message)s', datefmt='%Y-%m-%d %I:%M:%S %p')
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

    try:
        conn = psycopg2.connect(
            user=user,
            password=password,
            host=host,
            database=db
        )
        curs = conn.cursor(cursor_factory=psycopg2.extras.DictCursor)

        conn_internauta = utils.get_internauta_conn(log=log, DB_INTERNAUTI=DB_INTERNAUTI)
        cursor_internauta = conn_internauta.cursor(cursor_factory=psycopg2.extras.DictCursor)
        cursor_internauta.execute("SELECT id FROM baborg.aziende WHERE codice = %(codice_azienda)s", {
            "codice_azienda": codice_azienda
        })
        id_azienda = cursor_internauta.fetchone()["id"]
        cursor_internauta.close()

        curs.execute(
            "SELECT val_parametro::int <> 0 FROM bds_tools.parametri_pubblici pp WHERE pp.nome_parametro = 'fascicoliParlanti'")
        fascicoli_parlanti = curs.fetchone()[0]

        # Autocommit obbligatorio per stare in ascolto di notify.
        conn.set_isolation_level(psycopg2.extensions.ISOLATION_LEVEL_AUTOCOMMIT)

        log.info("Guardo se c'è del lavoro arretrato da fare subito")
        while True:
            #finche ho cose arretrate devo svolegere quelle una alla volta
            # devo
            curs.execute("LISTEN cannoneggiamenti_argo;")
            search_and_work(conn, codice_azienda, fascicoli_parlanti, conn_internauta, id_azienda)
            #time.sleep(10)

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

