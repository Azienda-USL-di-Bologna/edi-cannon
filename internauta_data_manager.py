import json
import psycopg2
import psycopg2.extras
from psycopg2.extras import Json

from db_connection import DB_CONFIG as db

from datetime import datetime

import queries_cannone as qc

conn = psycopg2.connect(
            user=db['internauta']['user'],
            password=db['internauta']['password'],
            host=db['internauta']['host'],
            database=db['internauta']['db']
        )

def get_aziende():
    q = "select * from baborg.aziende;"
    c = conn.cursor()
    c.execute(q)
    return c.fetchall();

def get_persona_by_cf(cf):
    q = "select * from baborg.persone where codice_fiscale = '%s'"
    c = conn.cursor()
    c.execute(q, (cf))
    return c.fetchall();

def delete_doc_list_row_by_guid_and_azienda(guid_documento, id_azienda):
    print("guid " + str(guid_documento) + " azienda " +  str(id_azienda))
    qDel = """delete from scripta.docs_list where guid_documento = %s and id_azienda = %s"""
    print(qDel % (guid_documento, id_azienda))
    c = conn.cursor()
    c.execute(qDel, (guid_documento, id_azienda))


STATI = { "None": None,
"PARERI": 'PARERE',
"REDAZIONE": 'REDAZIONE',
"SPEDIZIONE_MANUALE": 'SPEDIZIONE',
"ASPETTA_SPEDIZIONI": 'SPEDIZIONE',
"CONTROLLO_SEGRETERIA": 'CLASSIFICAZIONE',
"FINE": 'FINE',
"ATTENDI_JOBS": 'SPEDIZIONE',
"FIRMA": 'FIRMA'}
    #{REDAZIONE,CLASSIFICAZIONE,PARERE,VISTA,FIRMA,UFFICIO_ATTI,DG,DS,DA,DSC,SMISTAMENTO,SPEDIZIONE,FINE}


def upsert_doc_list_data(id_azienda, json_data):
    c = conn.cursor(cursor_factory=psycopg2.extras.DictCursor)
    try:
        c.execute(qc.insert_doc, {'id_azienda': id_azienda,
                            'guid_documento': json_data['guid_documento'],
                            'tipologia': json_data['tipologia'],
                            'open_command': "", 'command_type': "URL",
                            'id_persona_responsabile_procedimento': json_data['id_persona_responsabile_procedimento'],
                            'id_persona_redattrice': json_data['id_persona_redattrice'],
                            'id_struttura_registrazione': json_data['id_struttura_registrazione'],
                            'numero_proposta': json_data['numero_proposta'][5:],
                            'anno_proposta': json_data['numero_proposta'][0:3],
                            'numero_registrazione': json_data['numero_registrazione'],
                            'anno_registrazione': json_data['anno_registrazione'],
                            'data_creazione': json_data['data_creazione'],
                            'data_registrazione': json_data['data_registrazione'],
                            'data_pubblicazione': None,
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
                            }
                  )
        conn.commit()
    except Exception as ex:
        conn.rollback()
        print(ex)
        print(c.query)
        raise ex