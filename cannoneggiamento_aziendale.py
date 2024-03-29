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
        c.execute(qUpdate, (tuple(i for i in row['id_list']),))
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


def delete_cannoneggiamenti_done(row, conn):
    #log = logging.getLogger("cannoneggiamento_aziendale")
    log.info("delete_cannoneggiamenti_done")
    qDel = """ delete from esportazioni.cannoneggiamenti
    where id in %s """
    try:
        c = conn.cursor(cursor_factory=psycopg2.extras.DictCursor)
        log.info(qDel % str(tuple(i for i in row['id_list'])))
        c.execute(qDel, (tuple(i for i in row['id_list']),))
        log.info("delete_cannoneggiamenti_done eseguita con successo")
        conn.commit()
    except Exception as ex:
        log.error("delete_cannoneggiamenti_done fallita")
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


def get_nome(conn, id_fascicolo, parlante):
    #log = logging.getLogger("cannoneggiamento_aziendale")
    if parlante:
        log.info("get_nome : stringa vuota")
        return ''
    else:
        try:
            c = conn.cursor(cursor_factory=psycopg2.extras.DictCursor)
            query = """select nome_fascicolo from gd.fascicoligd where id_fascicolo = %(id_fascicolo)s"""
            c.execute(query, {'id_fascicolo': id_fascicolo})
            result = c.fetchone()
            log.info("get_nome: " + result['nome_fascicolo'])
            return result['nome_fascicolo']
        except Exception as ex:
            log.error("get_nome errore nel reperimento del nome")
            raise ex

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


def search_and_work(conn, codice_azienda):
    #log = logging.getLogger("cannoneggiamento_aziendale")
    log.info("Eseguo search_and_work....")
    qSel = """
        select distinct id_oggetto, tipo_oggetto, operazione, array_agg(id) as id_list
        from esportazioni.cannoneggiamenti
        where in_esecuzione = false
        group by id_oggetto, tipo_oggetto, operazione
    """
    try:
        c = conn.cursor(cursor_factory=psycopg2.extras.DictCursor)
        c.execute(qSel)
        rows = c.fetchall()
        c.execute("select val_parametro::int  <> 0 from bds_tools.parametri_pubblici pp where pp.nome_parametro = 'fascicoliParlanti'")
        parlante = c.fetchone()[0]
        
        conn_internauta = get_internauta_conn()
        cursor_internauta = conn_internauta.cursor(cursor_factory=psycopg2.extras.DictCursor)
        cursor_internauta.execute("select id from baborg.aziende where codice = %(codice_azienda)s", {
            "codice_azienda": codice_azienda
        })
        id_azienda = cursor_internauta.fetchone()["id"]
        
        if rows is not None and len(rows) > 0:
            for r in rows:
                try:
                    log.info("Tipologia %s" % str(r['tipo_oggetto']))
                    set_guids_in_esecuzione(r, conn)

                    if r['operazione'] == "DELETE":
                        log.info("cancello!")
                        idm.delete_doc_list_row_by_guid_and_azienda(r['id_oggetto'], codice_azienda, conn_internauta, id_azienda)
                        delete_cannoneggiamenti_done(r, conn)
                    elif r['tipo_oggetto'] == "pico":
                        if not got_delete_too(r, rows):
                            pico_data = argo_data_retriever.get_pico_document_by_guid(conn, r['id_oggetto'])
                            json_data = pico_data[0]
                            if json_data is not None:
                                idm.upsert_doc_list_data(codice_azienda, json_data, conn_internauta, id_azienda)
                        else:
                            log.info("dopo la devo cancellare, quindi skippo l'upsert di " + str(r['id_oggetto']))
                        delete_cannoneggiamenti_done(r, conn)
                    elif r['tipo_oggetto'] == "pico_pe":
                        if not got_delete_too(r, rows):
                            pico_data = argo_data_retriever.get_pico_pe_document_by_guid(conn, r['id_oggetto'])
                            json_data = pico_data[0]
                            if json_data is not None:
                                idm.upsert_doc_list_data(codice_azienda, json_data, conn_internauta, id_azienda)
                        else:
                            log.info("dopo la devo cancellare, quindi skippo l'upsert di " + str(r['id_oggetto']))
                        delete_cannoneggiamenti_done(r, conn)
                    elif r['tipo_oggetto'] == "pico_pu":
                        if not got_delete_too(r, rows):
                            pico_data = argo_data_retriever.get_pico_pu_document_by_guid(conn, r['id_oggetto'])
                            json_data = pico_data[0]
                            if json_data is not None:
                                idm.upsert_doc_list_data(codice_azienda, json_data, conn_internauta, id_azienda)
                        else:
                            log.info("dopo la devo cancellare, quindi skippo l'upsert di " + str(r['id_oggetto']))
                        delete_cannoneggiamenti_done(r, conn)
                    elif r['tipo_oggetto'] == "dete":
                        if not got_delete_too(r, rows):
                            dete_data = argo_data_retriever.get_dete_document_by_guid(conn, r['id_oggetto'])
                            json_data = dete_data[0]
                            if json_data is not None:
                                idm.upsert_doc_list_data(codice_azienda, json_data, conn_internauta, id_azienda)
                        else:
                            log.info("dopo la devo cancellare, quindi skippo l'upsert di " + str(r['id_oggetto']))
                        delete_cannoneggiamenti_done(r, conn)
                    elif r['tipo_oggetto'] == "deli":
                        if not got_delete_too(r, rows):
                            deli_data = argo_data_retriever.get_deli_document_by_guid(conn, r['id_oggetto'])
                            json_data = deli_data[0]
                            if json_data is not None:
                                idm.upsert_doc_list_data(codice_azienda, json_data, conn_internauta, id_azienda)
                        else:
                            log.info("dopo la devo cancellare, quindi skippo l'upsert di " + str(r['id_oggetto']))
                        delete_cannoneggiamenti_done(r, conn)
                    elif r['tipo_oggetto'] == "fascicolo":
                        nome = get_nome(conn, r['id_oggetto'], parlante)
                        idm.update_nome_fascicoli(nome, r['id_oggetto'], conn_internauta, id_azienda)
                        delete_cannoneggiamenti_done(r, conn)

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


def main(azienda, host, user, password, db):
    setta_log(azienda)
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
        finally:
            try:
                unlock(conn)
            except:
                pass