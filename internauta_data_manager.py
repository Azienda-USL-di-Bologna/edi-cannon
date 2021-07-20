#!/usr/bin/python3

import psycopg2
import psycopg2.extras
from psycopg2.extras import Json
from db_connection import DB_INTERNAUTI
from datetime import datetime
import queries_cannone as qc
import logging


def delete_doc_list_row_by_guid_and_azienda(guid_documento, codice_azienda):
    log = logging.getLogger("cannoneggiamento_aziendale")
    conn = get_internauta_conn()
    log.info("guid " + str(guid_documento) + " azienda " + str(codice_azienda))
    qDel = """
        delete from scripta.docs_list 
        where guid_documento = %(guid_documento)s 
        and id_azienda = (select id from baborg.aziende where codice = %(codice_azienda)s)
    """

    c = conn.cursor(cursor_factory=psycopg2.extras.DictCursor)
    c.execute(qDel, {
        "guid_documento": guid_documento,
        "codice_azienda": codice_azienda
    })
    log.info(c.query)
    c.close()
    conn.commit()


STATI = {
    "None": None,
    "PARERI": 'PARERE',
    "REDAZIONE": 'REDAZIONE',
    "SPEDIZIONE_MANUALE": 'SPEDIZIONE',
    "ASPETTA_SPEDIZIONI": 'SPEDIZIONE',
    "CONTROLLO_SEGRETERIA": 'CLASSIFICAZIONE',
    "FINE": 'FINE',
    "ATTENDI_JOBS": 'SPEDIZIONE',
    "Fine_Entrata": "FINE",
    "Bozza_Ricezione": "REDAZIONE",
    "Ricezione": "REDAZIONE",
    "FIRMA": 'FIRMA',
    "NUMERAZIONE": 'FINE'
}


def get_internauta_conn():
    log = logging.getLogger("cannoneggiamento_aziendale")
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


def upsert_doc_list_data(codice_azienda, json_data):
    log = logging.getLogger("cannoneggiamento_aziendale")
    conn = get_internauta_conn()
    c = conn.cursor(cursor_factory=psycopg2.extras.DictCursor)
    try:
        c.execute(qc.insert_doc, {
            'codice_azienda': codice_azienda,
            'guid_documento': json_data['guid_documento'],
            'tipologia': json_data['tipologia'],
            'open_command': json_data['url'],
            'command_type': "URL",
            'id_persona_responsabile_procedimento': json_data['id_persona_responsabile_procedimento'],
            'id_persona_redattrice': json_data['id_persona_redattrice'],
            'id_struttura_registrazione': json_data['id_struttura_registrazione'],
            'numero_proposta': json_data['numero_proposta'],
            'anno_proposta': json_data['anno_proposta'],
            'numero_registrazione': json_data['numero_registrazione'],
            'anno_registrazione': json_data['anno_registrazione'],
            'data_creazione': json_data['data_creazione'],
            'data_registrazione': json_data['data_registrazione'],
            'data_pubblicazione': json_data['data_pubblicazione'],
            'oggetto': json_data['oggetto'],
            'fascicolazioni': None if json_data['fascicolazioni'] is None else Json(json_data['fascicolazioni']),
            'classificazioni': None if json_data['classificazioni'] is None else Json(json_data['classificazioni']),
            'firmatari': None if json_data['firmatari'] is None else Json(json_data['firmatari']),
            'destinatari': None if json_data['destinatari'] is None else Json(json_data['destinatari']),
            'mittente': json_data['mittente'],
            'stato': STATI[str(json_data['stato'])],
            'visibilita_limitata': True if json_data['visibilita_limitata'] != 0 else False,
            'riservato': True if json_data['riservato'] != 0 else False,
            'annullato': True if json_data['annullato'] != 0 else False,
            'protocollo_esterno': json_data['protocollo_esterno'],
            'mail_collegio': json_data['mail_collegio'],
            'stato_ufficio_atti': json_data['stato_ufficio_atti'],
            'data_inserimento_riga': datetime.now(),
            'persone_vedenti': None if json_data['persone_vedenti'] is None else Json(json_data['persone_vedenti']),
            'id_mezzo_ricezione': json_data['id_mezzo_ricezione'],
            'id_strutture_firmatari': json_data['id_strutture_firmatari'],
            'sulla_scrivania_di': json_data['sulla_scrivania_di']
        })
        log.info(c.query)
        conn.commit()

    except Exception as ex:
        conn.rollback()
        log.error("errore in upsert_doc_list_data")
        log.error(ex)
        log.error(c.query)
        raise ex