#!/usr/bin/python3
# -*- coding: utf-8 -*-

import io
import logging
import select
import sys
import time
import traceback
import psycopg2
import doc_list_data_retriever
import internauta_data_manager as idm
log = None

def try_lock(conn):
    q = "select pg_try_advisory_lock(2243247306)"
    c = conn.cursor()
    c.execute(q)
    return c.fetchone()[0]


def take_lock(conn):
    q = "select pg_advisory_lock(2243247306)"
    c = conn.cursor()
    c.execute(q)
    return c.fetchone()[0]


def got_delete_too(row, rows):
    got_to_delete_the_row = False;
    for r in rows:
        if(row[0] == r[0] and row[1] == r[1] and r[2] == "DELETE"):
            got_to_delete_the_row = True
            break
    return got_to_delete_the_row


def set_guids_in_esecuzione(row, conn):
    qUpdate = """ update esportazioni.cannoneggiamenti
    set in_esecuzione = true
    where id in %s """
    c = conn.cursor()
    print(qUpdate % str(tuple(i for i in row[3])))
    c.execute(qUpdate, (tuple(i for i in row[3]),))


def set_guids_in_error(row, conn):
    qError = """ update esportazioni.cannoneggiamenti
    set in_error = true
    where id in %s """
    c = conn.cursor()
    print(qError % str(tuple(i for i in row[3])))
    c.execute(qError, (tuple(i for i in row[3]),))


def delete_cannoneggiamenti_done(row, conn):
    log = logging.getLogger("cannoneggiamento_aziendale")
    log.info("delete_cannoneggiamenti_done")
    qDel = """ delete from esportazioni.cannoneggiamenti
    where id in %s """
    c = conn.cursor()
    print(qDel % str(tuple(i for i in row[3])))
    c.execute(qDel, (tuple(i for i in row[3]),))
    log.info(c.query)
    conn.commit()


def search_and_work(conn, codice_azienda):
    log = logging.getLogger("cannoneggiamento_aziendale")
    log.info("Eseguo search_and_work....")

    qSel = """
        select distinct id_oggetto, tipo_oggetto, operazione, array_agg(id) 
        from esportazioni.cannoneggiamenti
        where in_esecuzione = false
        group by id_oggetto, tipo_oggetto, operazione
    """
    c = conn.cursor()
    c.execute(qSel)
    rows = c.fetchall()
    if rows is not None and len(rows) > 0:
        for r in rows:
            try:
                log.info(r)
                log.info("Tipologia %s" % str(r[1]))
                set_guids_in_esecuzione(r, conn)
                if r[1] == "pico":
                    if r[2] == "DELETE":
                        log.info("cancello!")
                        idm.delete_doc_list_row_by_guid_and_azienda(r[0], codice_azienda)
                    else:
                        if not got_delete_too(r, rows):
                            pico_data = doc_list_data_retriever.get_edi_data_from_pico(conn, r[0])
                            json_data = pico_data[0]
                            if json_data is not None:
                                idm.upsert_doc_list_data(codice_azienda, json_data)
                        else:
                            log.info("dopo la devo cancellare, quindi skippo l'upsert di " + str(r[0]))
                        # TO_DO: Cancelliamo le righe già fatte da cannoneggiamenti_argo se è andato tutto ok?
                            #delete_cannoneggiamenti_done(r, conn)
            except Exception as ex:
                print("Erroro!  search_and_work")
                log.error("ERRORE! SEARCH_AND_WORK")
                print(ex)
                log.error(ex)
                raise ex
                set_guids_in_error(r, conn)


def main(azienda, host, user, password, db):
    filename = "edi_cannon_" + str(azienda) + ".log"
    logging.basicConfig(
        filename=filename,
        level=logging.INFO,
        format='%(asctime)s %(message)s',
        datefmt='%Y-%m-%d %I:%M:%S %p'
    )
    log = logging.getLogger("cannoneggiamento_aziendale")

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

            curs = conn.cursor()
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
