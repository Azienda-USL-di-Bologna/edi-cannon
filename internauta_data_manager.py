#!/usr/bin/python3
import os
import psycopg2
import psycopg2.extras
from psycopg2.extras import Json
from datetime import datetime
from dizionari import RUOLO_ATTORE, SOTTOTIPO_ALLEGATO, STATI, STATI_UFFICIO_ATTI, TIPO_ALLEGATO
import queries_cannone as qc
import logging
from io import StringIO
import traceback
import sys
import cannoneggiamento_aziendale
import time

log = logging.getLogger("cannoneggiamento_aziendale")

"""
    Cancello il documento by guid
"""
def delete_doc_by_guid_and_azienda(guid_documento, conn, id_azienda):
    try:
        c = conn.cursor(cursor_factory=psycopg2.extras.DictCursor)
        c.execute(qc.delete_doc, {
            "guid_documento": guid_documento,
            "id_azienda": id_azienda
        })
        c.close()
        conn.commit()
    except Exception as ex:
        conn.rollback()
        log.error("delete_doc_by_guid_and_azienda fallita")
        log.error(c.query)
        raise ex


"""
    A partire da un grosso json che contiene i dati del documento effettua la upsert per il documento
    Dopodiche aggiorna persone vedenti e allegati. Per questi ultimi si connette a minio
"""
def upsert_doc_list_data(codice_azienda, json_data, conn, id_azienda):
    #log = logging.getLogger("cannoneggiamento_aziendale")
    c = conn.cursor(cursor_factory=psycopg2.extras.DictCursor)
    try:
        # AGGIORNAMENTO DEL DOC
        if json_data['id_pec_mittente'] is not None:
            c.execute(qc.get_id_pec, {'id_pec_mittente': json_data['id_pec_mittente']})
            id_pec_mittente = c.fetchone()["id"]
        now = time.time()
        c.execute(qc.insert_doc, {
            'id_azienda': id_azienda,
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
            'firmatari': None if json_data['firmatari'] is None else Json(json_data['firmatari']),
            'destinatari': None if json_data['destinatari'] is None else Json(json_data['destinatari']),
            'mittente': json_data['mittente'],
            'stato': "ANNULLATO" if json_data['annullato'] != 0 else STATI[str(json_data['stato'])],
            'visibilita_limitata': True if json_data['visibilita_limitata'] != 0 else False,
            'riservato': True if json_data['riservato'] != 0 else False,
            'annullato': True if json_data['annullato'] != 0 else False,
            'protocollo_esterno': json_data['protocollo_esterno'],
            'mail_collegio': json_data['mail_collegio'],
            'stato_ufficio_atti': STATI_UFFICIO_ATTI[str(json_data['stato_ufficio_atti'])],
            'data_inserimento_riga': datetime.now(),
            #'persone_vedenti': None if json_data['persone_vedenti'] is None else Json(json_data['persone_vedenti']),
            'id_mezzo_ricezione': json_data['id_mezzo_ricezione'],
            'id_strutture_segreteria': json_data['id_strutture_segreteria'],
            'sulla_scrivania_di': None if json_data['sulla_scrivania_di'] is None else Json(json_data['sulla_scrivania_di']),
            'id_applicazione': json_data['id_applicazione'],
            'version': json_data['version'], 
            'conservazione': json_data['conservazione'],
            'id_pec_mittente': None if id_pec_mittente is None else id_pec_mittente
        })
        later = time.time()
        difference_upsert = int(later - now)
 
        id_doc = c.fetchone()["id"]


        # AGGIORNAMENTO DELLE PERSONE VEDENTI 
        now = time.time()
        values_persone_vedenti = ""
        if json_data['persone_vedenti'] is not None and len(json_data['persone_vedenti']) > 0:
            for persona_vedente in json_data['persone_vedenti']:
                values_persone_vedenti = values_persone_vedenti + f"""(
                    {persona_vedente["idPersona"]}, 
                    {persona_vedente['mioDocumento']}, 
                    {persona_vedente['pienaVisibilita']}, 
                    {"'" + persona_vedente['modalitaApertura'] + "'" if ('modalitaApertura' in persona_vedente) else 'null'}
                ),"""
        if len(values_persone_vedenti) > 0:
            # Chiamo la upsert and delete
            values_persone_vedenti = values_persone_vedenti[:-1] # rimuovo l'ultima virgola
            c.execute(qc.upsert_persone_vedenti_and_delete_the_others.format(values=values_persone_vedenti), {
                "guid_documento": json_data['guid_documento'],
                "id_persona": persona_vedente["idPersona"],
                "data_registrazione": json_data['data_registrazione'],
                "id_azienda": id_azienda,
                "id_doc": id_doc
            })
        else:
            # Faccio solo la delete
            c.execute(qc.delete_persone_vedenti, {
                "guid_documento": json_data['guid_documento'],
                "id_azienda": id_azienda,
                'data_creazione': json_data['data_creazione']
            })
        later = time.time()
        difference_persone_vedenti = int(later - now)


        # AGGIORNAMENTO DEGLI ATTORI
        now = time.time()
        values_attori = ""
        if json_data['attori'] is not None and len(json_data['attori']) > 0:
            for attore in json_data['attori']:
                # idStruttura può essere null solo perché nei vecchi attori non si riescie a fare il match con le strutture internuata
                values_attori = values_attori + f"""(
                    {attore["idPersona"]}, 
                    {attore['idStruttura'] if attore['idStruttura'] is not None else 'null'}, 
                    {"'" + RUOLO_ATTORE[attore['ruolo']] + "'"}, 
                    {attore['ordinale'] if attore['ordinale'] is not None else 'null'}
                ),"""
        if len(values_attori) > 0:
            # Chiamo la upsert and delete
            values_attori = values_attori[:-1] # rimuovo l'ultima virgola
            c.execute(qc.upsert_attori_and_delete_the_others.format(values=values_attori), {
                "id_doc": id_doc
            })
        else:
            # Faccio solo la delete
            c.execute(qc.delete_attori, {
                "id_doc": id_doc
            })
        later = time.time()
        difference_attori = int(later - now)


        # AGGIORNAMENTO DEGLI ALLEGATI
        now = time.time()
        if json_data['allegati'] is not None and len(json_data['allegati']) > 0:
            mongo_uuids = []
            for allegato in json_data['allegati']:
                uid_repository = allegato['uid_repository']
                if uid_repository['uid_pdf'] is not None:
                    mongo_uuids += [uid_repository['uid_pdf']]
                if uid_repository['uid_firmato'] is not None:
                    mongo_uuids += [uid_repository['uid_firmato']]
                if uid_repository['uid_originale'] is not None:
                    mongo_uuids += [uid_repository['uid_originale']]
            if not mongo_uuids == []:
                minio_conn = cannoneggiamento_aziendale.get_minirepo_conn()
                m = minio_conn.cursor(cursor_factory=psycopg2.extras.DictCursor)
                m.execute(qc.query_minio, {
                    "mongo_uuids": mongo_uuids
                })
                uuids_map = m.fetchone()['res']
                if uuids_map is not None:
                    for key in uuids_map.keys():
                        obj = uuids_map[key]
                        obj['estensione'] = os.path.splitext(obj['nome'])[1][1:]
                        obj['dataCreazione'] = json_data['data_creazione']
                        obj['mimeType'] = None
                minio_conn.close()

            for allegato in json_data['allegati']:
                allegato['firmato'] = False
                allegato['dettagli'] = {}
                uid_repository = allegato['uid_repository']
                if uid_repository['uid_pdf'] is not None:
                    uid_pdf = uid_repository['uid_pdf']
                    if uuids_map is not None:
                        if uid_pdf in uuids_map:
                            dettaglio_pdf = uuids_map[uid_pdf]
                            allegato['dettagli']['convertito'] = dettaglio_pdf
                if uid_repository['uid_firmato'] is not None:
                    uid_firmato = uid_repository['uid_firmato']
                    if uuids_map is not None:
                        if uid_firmato in uuids_map:
                            dettaglio_firmato = uuids_map[uid_firmato]
                            allegato['dettagli']['originaleFirmato'] = dettaglio_firmato
                            allegato['firmato'] = True
                if uid_repository['uid_originale'] is not None:
                    uid = uid_repository['uid_originale']
                    if uuids_map is not None:
                        if uid in uuids_map:
                            dettaglio_originale = uuids_map[uid]
                            allegato['dettagli']['originale'] = dettaglio_originale
                c.execute(qc.insert_allegati_doc, {
                    "nome": allegato['nome'],
                    "tipo": TIPO_ALLEGATO[allegato['tipo_allegato']],
                    "principale": allegato['principale'],
                    "firmato": allegato['firmato'],
                    "ordinale": allegato['ordinale'],
                    "guid_documento": json_data['guid_documento'],
                    "id_allegato_padre": allegato['id_allegato_padre'],
                    "data_inserimento": allegato['data_inserimento'],
                    "dettagli": Json(allegato['dettagli']),
                    "id_esterno": allegato['id_allegato_argo'],
                    "sottotipo": None if allegato["sottotipo"] is None else SOTTOTIPO_ALLEGATO[allegato["sottotipo"]],
                    "additional_data": None if allegato["additional_data"] is None else Json(allegato["additional_data"])
                })
        later = time.time()
        difference_allegati = int(later - now)

        # DOCUMENTO AGGIORNATO. COMMITTO
        conn.commit()
        log.info(f"upsert_doc_list_data eseguita con successo per documento con guid: {json_data['guid_documento']}")
        log.info("%s secondi upsert, %s secondi pers.vedenti, %s secondi allegati, %s secondi difference_attori" % (str(difference_upsert), str(difference_persone_vedenti), str(difference_allegati), str(difference_attori)))
    except Exception as ex:
        conn.rollback()
        log.error(f"errore in upsert_doc_list_data per guid {json_data['guid_documento']}")
        log.error(ex)
        log.error(c.query)
        output = StringIO()
        traceback.print_exception(*sys.exc_info(), limit=None, file=output)
        log.critical(output.getvalue())
        traceback.print_exception(*sys.exc_info())
        raise ex
