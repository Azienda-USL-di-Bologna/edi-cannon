#!/usr/bin/python3

import psycopg2
import psycopg2.extras
from psycopg2.extras import Json
from db_connection import DB_INTERNAUTI
from datetime import datetime
import queries_cannone as qc
import logging
log = logging.getLogger("cannoneggiamento_aziendale")


def update_nome_fascicoli(nome, id_oggetto):
    log.info("update_nome_fascicoli")
    qupdate = """select scripta.update_nome_fascicolo_from_idfascicoloargo(%(nome)s,%(id_fascicolo)s)"""
    try:
        conn = get_internauta_conn()
        c = conn.cursor(cursor_factory=psycopg2.extras.DictCursor)
        c.execute(qupdate, {'nome': nome,
                        'id_fascicolo': id_oggetto})
        conn.commit()
        log.info("update_nome_fascicoli eseguita con successo")
    except Exception as ex:
        log.error("update_nome_fascicoli fallita")
        log.error(ex)
        log.error(c.query)


def delete_doc_list_row_by_guid_and_azienda(guid_documento, codice_azienda):
    #log = logging.getLogger("cannoneggiamento_aziendale")
    conn = get_internauta_conn()
    log.info("delete_doc_list_row_by_guid_and_azienda di guid " + str(guid_documento) + " con azienda " + str(codice_azienda))
    qDel = """
        delete from scripta.docs_list 
        where guid_documento = %(guid_documento)s 
        and id_azienda = (select id from baborg.aziende where codice = %(codice_azienda)s)
    """
    try:
        c = conn.cursor(cursor_factory=psycopg2.extras.DictCursor)
        c.execute(qDel, {
            "guid_documento": guid_documento,
            "codice_azienda": codice_azienda
        })
        c.close()
        conn.commit()
        log.info("delete_doc_list_row_by_guid_and_azienda eseguita con successo")
    except Exception as ex:
        conn.rollback()
        log.error("delete_doc_list_row_by_guid_and_azienda fallita")
        log.error(c.query)
        raise ex


STATI = {
    "None": None,
    "REDAZIONE": 'REDAZIONE',
    "Redazione": 'REDAZIONE',
    "Bozza_Redazione": "REDAZIONE",
    "Bozza_Ricezione": "REDAZIONE",
    "Bozza": "REDAZIONE",
    "Ricezione": "REDAZIONE",
    "PARERI": 'PARERE',
    "Pareri": 'PARERE',
    "Visti": "VISTA",
    "Approvazione": 'FIRMA',
    "FIRMA": 'FIRMA',
    "Firma": 'FIRMA',
    "SPEDIZIONE_MANUALE": 'SPEDIZIONE_MANUALE',
    "ASPETTA_SPEDIZIONI": 'ASPETTA_SPEDIZIONI',
    "CONTROLLO_SEGRETERIA": 'CONTROLLO_SEGRETERIA',
    "FINE": 'FINE',
    "Fine": 'FINE',
    "Fine_Emergenza_In": "FINE",
    "ATTENDI_JOBS": 'ATTENDI_JOBS',
    "Fine_Entrata": "FINE",
    "SMISTAMENTO": "SMISTAMENTO",
    "NUMERAZIONE": 'NUMERAZIONE',
    "Registrazione_Protocollo": 'REGISTRAZIONE_PROTOCOLLO',
    "AVVIA_SPEDIZIONI": 'AVVIA_SPEDIZIONI'
}

STATI_UFFICIO_ATTI = {
    "None": None,
    "sospesa": "SOSPESA",
    "Elaborata": "ELABORATA",
    "da_valutare": "DA_VALUTARE"
}


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


def upsert_doc_list_data(codice_azienda, json_data):
    #log = logging.getLogger("cannoneggiamento_aziendale")
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
            'stato_ufficio_atti': STATI_UFFICIO_ATTI[str(json_data['stato_ufficio_atti'])],
            'data_inserimento_riga': datetime.now(),
            'persone_vedenti': None if json_data['persone_vedenti'] is None else Json(json_data['persone_vedenti']),
            'id_mezzo_ricezione': json_data['id_mezzo_ricezione'],
            'id_strutture_segreteria': json_data['id_strutture_segreteria'],
            'sulla_scrivania_di': json_data['sulla_scrivania_di'],
            'id_applicazione': json_data['id_applicazione'],
            'version': json_data['version']
        })
        conn.commit()
        log.info(f"upsert_doc_list_data eseguita con successo per documento con guid: {json_data['guid_documento']}")
    except Exception as ex:
        conn.rollback()
        log.error(f"errore in upsert_doc_list_data per guid {json_data['guid_documento']}")
        log.error(ex)
        log.error(c.query)
        raise ex