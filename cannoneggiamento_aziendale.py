#!/usr/bin/python3
# -*- coding: utf-8 -*-

import io
import logging
import select
import sys
import time
import traceback
import psycopg2
import psycopg2.extras
from psycopg2.extras import Json
import doc_list_data_retriever
import internauta_data_manager as idm
from logging.handlers import TimedRotatingFileHandler
log = None

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


def got_delete_too(row, rows):
    got_to_delete_the_row = False;
    for r in rows:
        if(row['id_oggetto'] == r['id_oggetto'] and row['tipo_oggetto'] == r['tipo_oggetto'] and r['operazione'] == "DELETE"):
            got_to_delete_the_row = True
            break
    return got_to_delete_the_row


def set_guids_in_esecuzione(row, conn):
    qUpdate = """ update esportazioni.cannoneggiamenti
    set in_esecuzione = true
    where id in %s """
    c = conn.cursor(cursor_factory=psycopg2.extras.DictCursor)
    print(qUpdate % str(tuple(i for i in row['id_list'])))
    c.execute(qUpdate, (tuple(i for i in row['id_list']),))


def set_guids_in_error(row, conn, codice_azienda, ex):
    log = logging.getLogger("cannoneggiamento_aziendale")
    log.info('setto guid in errore')
    qError = """ update esportazioni.cannoneggiamenti
    set in_error = true
    where id in %s """
    c = conn.cursor(cursor_factory=psycopg2.extras.DictCursor)
    print(qError % str(tuple(i for i in row[3])))
    c.execute(qError, (tuple(i for i in row[3]),))
    conn.commit()
    erroro(conn, codice_azienda, ex)

def delete_cannoneggiamenti_done(row, conn):
    log = logging.getLogger("cannoneggiamento_aziendale")
    log.info("delete_cannoneggiamenti_done")
    qDel = """ delete from esportazioni.cannoneggiamenti
    where id in %s """
    c = conn.cursor(cursor_factory=psycopg2.extras.DictCursor)
    print(qDel % str(tuple(i for i in row['id_list'])))
    c.execute(qDel, (tuple(i for i in row['id_list']),))
    log.info(c.query)
    conn.commit()

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


def get_nome(conn, id_fascicolo, parlante):
    log = logging.getLogger("cannoneggiamento_aziendale")
    c = conn.cursor(cursor_factory=psycopg2.extras.DictCursor)
    query = """select nome_fascicolo from gd.fascicoligd where id_fascicolo = %(id_fascicolo)s"""
    c.execute(query, {'id_fascicolo': id_fascicolo})
    result = c.fetchone()
    if parlante:
        log.info("get_nome : stringa vuota")
        return ''
    else:
        log.info("get_nome " + result['nome_fascicolo'])
        return result['nome_fascicolo']


def search_and_work(conn, codice_azienda):
    log = logging.getLogger("cannoneggiamento_aziendale")
    log.info("Eseguo search_and_work....")

    qSel = """
        select distinct id_oggetto, tipo_oggetto, operazione, array_agg(id) as id_list
        from esportazioni.cannoneggiamenti
        where in_esecuzione = false
        group by id_oggetto, tipo_oggetto, operazione
    """
    c = conn.cursor(cursor_factory=psycopg2.extras.DictCursor)
    c.execute(qSel)
    rows = c.fetchall()
    c.execute("select val_parametro::int  <> 0 from bds_tools.parametri_pubblici pp where pp.nome_parametro = 'fascicoliParlanti'")
    parlante = c.fetchone()[0]
    if rows is not None and len(rows) > 0:
        for r in rows:
            try:
                log.info(r)
                log.info("Tipologia %s" % str(r['tipo_oggetto']))
                set_guids_in_esecuzione(r, conn)
                if r['tipo_oggetto'] == "pico":
                    if r['operazione'] == "DELETE":
                        log.info("cancello!")
                        idm.delete_doc_list_row_by_guid_and_azienda(r['id_oggetto'], codice_azienda)
                    else:
                        if not got_delete_too(r, rows):
                            pico_data = doc_list_data_retriever.get_edi_data_from_pico(conn, r['id_oggetto'])
                            json_data = pico_data[0]
                            if json_data is not None:
                                idm.upsert_doc_list_data(codice_azienda, json_data)
                        else:
                            log.info("dopo la devo cancellare, quindi skippo l'upsert di " + str(r['id_oggetto']))
                        # TO_DO: Cancelliamo le righe già fatte da cannoneggiamenti_argo se è andato tutto ok?
              #              delete_cannoneggiamenti_done(r, conn, codice_azienda)
                elif r['tipo_oggetto'] == "fascicolo":
                    nome = get_nome(conn, r['id_oggetto'],parlante)
                    idm.update_nome_fascicoli(nome, r['id_oggetto'])


            except Exception as ex:
                print("Erroro!  search_and_work")
                log.error("ERRORE! SEARCH_AND_WORK")
                print(ex)
                log.error(ex)
                #raise ex
                errore = ex.args[0]
                erre = r
                connessone = conn
                codiceazz = codice_azienda
                set_guids_in_error(r, conn, codice_azienda, errore)


def main(azienda, host, user, password, db):

    filename = "log/edi_cannon_" + str(azienda) + ".log"
    log = logging.getLogger("cannoneggiamento_aziendale")
    fmt = logging.Formatter('%(asctime)s %(message)s', datefmt='%Y-%m-%d %I:%M:%S %p')
    hnd = TimedRotatingFileHandler(filename, when='midnight', interval=1, backupCount=7)
    hnd.setFormatter(fmt)
    log.addHandler(hnd)
    log.setLevel(logging.INFO)

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

            search_and_work(conn, azienda)

            curs = conn.cursor(cursor_factory=psycopg2.extras.DictCursor)
            curs.execute("LISTEN cannoneggiamenti_argo;")
            log.info("In ascolto per altro lavoro..")

            while 1:
                if select.select([conn], [], []):
                    log.info("Ricevuta notifica di altro lavoro")
                    conn.poll()
                    while conn.notifies:
                        del conn.notifies[:]
                        search_and_work(conn, azienda)

        except:
            log.error("Probabile errore nella connessione al db o nell'esecuzione del lavoro.")
            output = io.StringIO()
            traceback.print_exception(*sys.exc_info(), limit=None, file=output)
            log.error(output.getvalue())
            time.sleep(10)
