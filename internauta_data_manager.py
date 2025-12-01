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
import re

log = logging.getLogger("cannoneggiamento_aziendale")
map_collegi_sindcali = {}
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


def disable_enable_trigger_update_doc_detail(dst_conn, action):
    c = dst_conn.cursor(cursor_factory=psycopg2.extras.DictCursor)
    
    log.info(f"{action} dei trigger")
    
    # Template della query
    query = """
        ALTER TABLE scripta.attori_docs {action} TRIGGER update_doc_detail;
    """
    # query = """
    #     --ALTER TABLE scripta.docs {action} TRIGGER update_doc_detail;
    #     ALTER TABLE scripta.attori_docs {action} TRIGGER update_doc_detail;
    #     --ALTER TABLE scripta.collegi_sindacali_docs {action} TRIGGER update_doc_detail;
    #     --ALTER TABLE scripta.registri_docs {action} TRIGGER update_doc_detail;
    #     --ALTER TABLE scripta.related {action} TRIGGER update_doc_detail;
    #     --ALTER TABLE scripta.spedizioni {action} TRIGGER update_doc_detail;
    # """
    
    # Formatta la query sostituendo {action} con ENABLE o DISABLE
    formatted_query = query.format(action=action)
    
    c.execute(formatted_query)
    c.close()


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
            connex = conn.cursor(cursor_factory=psycopg2.extras.DictCursor)
            connex.execute(qc.get_id_pec, {'id_pec_mittente': json_data['id_pec_mittente']})
            json_data['id_pec_mittente'] = connex.fetchone()["id"]
            log.info(f"pec mittente è: {json_data['id_pec_mittente']}")
        now = time.time()
        query_to_use = qc.insert_doc
        id_doc = None

        

        if 'id_doc' in json_data and json_data['id_doc'] is not None:
            query_to_use = qc.update_doc_by_id
            id_doc = json_data['id_doc']
            log.info("ho l'id_doc %s, userò l'update" % str(id_doc))
        c.execute(query_to_use, {
            'id_azienda': id_azienda,
            'guid_documento': json_data['guid_documento'],
            'tipologia': json_data['tipologia'],
            'id_persona_responsabile_procedimento': json_data['id_persona_responsabile_procedimento'],
            'id_persona_redattrice': json_data['id_persona_redattrice'],
            'id_struttura_registrazione': json_data['id_struttura_registrazione'],
            'numero_proposta': json_data['numero_proposta'],
            'anno_proposta': json_data['anno_proposta'],
            'numero_registrazione': json_data['numero_registrazione'],
            'anno_registrazione': json_data['anno_registrazione'],
            'data_creazione': json_data['data_creazione'],
            'data_registrazione': json_data['data_registrazione'],
            'oggetto': json_data['oggetto'],
            'testo': json_data['testo'],
            'stato': "ANNULLATO" if json_data['annullato'] is not None else STATI[str(json_data['stato'])],
            'visibilita_limitata': json_data['visibilita_limitata'],
            'riservato': json_data['riservato'],
            'annullato': json_data['annullato'] is not None,
            'protocollo_esterno': json_data['protocollo_esterno'],
            #'mail_collegio': json_data['mail_collegio'],
            'data_inserimento_riga': datetime.now(),
            #'persone_vedenti': None if json_data['persone_vedenti'] is None else Json(json_data['persone_vedenti']),
            #'id_mezzo_ricezione': json_data['id_mezzo_ricezione'],
            'id_applicazione': json_data['id_applicazione'],
            'version': json_data['version'],
            'additional_data': Json(json_data['additional_data']),
            'id_pec_mittente': None if json_data['id_pec_mittente'] is None else json_data['id_pec_mittente'],
            'id_doc': id_doc
        })
        later = time.time()
        difference_upsert = int(later - now)

        id_doc = c.fetchone()["id"]

        


        # OLD AGGIORNAMENTO DELLE PERSONE VEDENTI  - ORA LO FACCIO PIU SOTTO
        # now = time.time()
        # values_persone_vedenti = ""
        # if json_data['persone_vedenti'] is not None and len(json_data['persone_vedenti']) > 0:
        #     for persona_vedente in json_data['persone_vedenti']:
        #         values_persone_vedenti = values_persone_vedenti + f"""(
        #             {persona_vedente["idPersona"]}, 
        #             {persona_vedente['mioDocumento']}, 
        #             {persona_vedente['pienaVisibilita']}, 
        #             {"'" + persona_vedente['modalitaApertura'] + "'" if ('modalitaApertura' in persona_vedente) else 'null'}
        #         ),"""
        # if len(values_persone_vedenti) > 0:
        #     # Chiamo la upsert and delete
        #     values_persone_vedenti = values_persone_vedenti[:-1] # rimuovo l'ultima virgola
        #     c.execute(qc.upsert_persone_vedenti_and_delete_the_others.format(values=values_persone_vedenti), {
        #         "guid_documento": json_data['guid_documento'],
        #         "id_persona": persona_vedente["idPersona"],
        #         "data_registrazione": json_data['data_registrazione'],
        #         "id_azienda": id_azienda,
        #         "id_doc": id_doc
        #     })
        # else:
        #     # Faccio solo la delete
        #     c.execute(qc.delete_persone_vedenti, {
        #         "guid_documento": json_data['guid_documento'],
        #         "id_azienda": id_azienda,
        #         'data_creazione': json_data['data_creazione']
        #     })
        # later = time.time()
        # difference_persone_vedenti = int(later - now)

        #AGGIORNAMENTO REGISTRAZIONI
        if(json_data['numero_registrazione'] is None):
            now = time.time()
            #c.execute(qc.insert_registri_docproposte, {
            #        "id_doc": id_doc,
            #        "numero_proposta": json_data["numero_proposta"],
            #        "anno_proposta": json_data["anno_proposta"],
            #        "id_persona_registrazione": json_data["id_persona_registrazione"],
            #        "id_struttura_registrazione": json_data["id_struttura_registrazione"],
            #        "data_creazione": json_data["data_creazione"],
            #        "tipologia": json_data["tipologia"],
            #        "id_azienda": id_azienda
            #    }
            #)
            later = time.time()
            difference_registrazioni = int(later - now)
        else:
            now = time.time()
            #c.execute(qc.insert_registri_docproposte, {
            #    "id_doc": id_doc,
            #    "numero_proposta": json_data["numero_proposta"],
            #    "anno_proposta": json_data["anno_proposta"],
            #    "id_persona_registrazione": json_data["id_persona_registrazione"],
            #    "id_struttura_registrazione": json_data["id_struttura_registrazione"],
            #    "data_creazione": json_data["data_creazione"],
            #    "tipologia": json_data["tipologia"],
            #    "id_azienda": id_azienda
            #}
            #)
            c.execute(qc.insert_registri_doc_registrati, {
                "id_doc": id_doc,
                "numero_registrazione": json_data["numero_registrazione"],
                "anno_registrazione": json_data["anno_registrazione"],
                "id_persona_registrazione": json_data["id_persona_registrazione"],
                "id_struttura_registrazione": json_data["id_struttura_registrazione"],
                "data_registrazione": json_data["data_registrazione"],
                "tipologia": json_data["tipologia"],
                "id_azienda": id_azienda
            })
            later = time.time()
            difference_registrazioni = int(later - now)

        # AGGIORNAMENTO DEGLI ALLEGATI
        now = time.time()
        id_allegati_da_tenere = []
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
                    # Ciclo gli uuid, ogni uuid rappresenta una certa versione di un certo allegato
                    for key in uuids_map.keys():
                        obj = uuids_map[key]
                        obj['estensione'] = os.path.splitext(obj['nome'])[1][1:]
                        #obj['dataCreazione'] = json_data['data_creazione']
                        #obj['mimeType'] = allegato['mime_type']
                minio_conn.close()

            for allegato in json_data['allegati']:
                allegato['firmato'] = False
                allegato['dettagli'] = {}
                uid_repository = allegato['uid_repository']
                if uid_repository['uid_pdf'] is not None:
                    uid_pdf = uid_repository['uid_pdf']
                    if uuids_map is not None:
                        if uid_pdf in uuids_map:
                            uuids_map[uid_pdf]["mimeType"] = "application/pdf"
                            uuids_map[uid_pdf]["dataCreazione"] = allegato['data_inserimento']
                            dettaglio_pdf = uuids_map[uid_pdf]
                            dettaglio_pdf["classz"] = "it.bologna.ausl.model.entities.scripta.Allegato$DettaglioAllegato"
                            allegato['dettagli']['convertito'] = dettaglio_pdf
                if uid_repository['uid_firmato'] is not None:
                    uid_firmato = uid_repository['uid_firmato']
                    if uuids_map is not None:
                        if uid_firmato in uuids_map:
                            uuids_map[uid_firmato]["mimeType"] = "application/pdf"  # TODO: Qui ci andrebbe il corretto mimetype del file firmato, da tirar su con le stored procedue
                            uuids_map[uid_firmato]["dataCreazione"] = allegato['data_inserimento']
                            dettaglio_firmato = uuids_map[uid_firmato]
                            dettaglio_firmato["classz"] = "it.bologna.ausl.model.entities.scripta.Allegato$DettaglioAllegato"
                            allegato['dettagli']['originaleFirmato'] = dettaglio_firmato
                            allegato['firmato'] = True
                if uid_repository['uid_originale'] is not None:
                    uid = uid_repository['uid_originale']
                    if uuids_map is not None:
                        if uid in uuids_map:
                            uuids_map[uid]["mimeType"] = allegato['mime_type']
                            uuids_map[uid]["dataCreazione"] = allegato['data_inserimento']
                            dettaglio_originale = uuids_map[uid]
                            dettaglio_originale["classz"] = "it.bologna.ausl.model.entities.scripta.Allegato$DettaglioAllegato"
                            allegato['dettagli']['originale'] = dettaglio_originale
                        else:
                            # Qui non dovrei entrare perché se non entro allora il file è andato perduto, cioè non l'ho torvato su minirepo
                            # In ogni caso voglio scrivere ciò che so dell'originale
                            dettaglio_originale = {
                                "mimeType": allegato['mime_type'],
                                "dataCreazione": allegato['data_inserimento'],
                                "nome": allegato["nome"],
                                "classz": "it.bologna.ausl.model.entities.scripta.Allegato$DettaglioAllegato"
                            }
                            allegato['dettagli']['originale'] = dettaglio_originale
                c.execute(qc.insert_allegati_doc, {
                    "nome": allegato['nome'],
                    "tipo": TIPO_ALLEGATO[allegato['tipo_allegato']],
                    "principale": allegato['principale'],
                    "firmato": allegato['firmato'],
                    "ordinale": allegato['ordinale'],
                    "id_doc": id_doc,
                    "id_allegato_padre": allegato['id_allegato_padre'],
                    "data_inserimento": allegato['data_inserimento'],
                    "dettagli": Json(allegato['dettagli']),
                    "id_esterno": allegato['id_allegato_argo'],
                    "sottotipo": None if allegato["sottotipo"] is None else SOTTOTIPO_ALLEGATO[allegato["sottotipo"]],
                    "additional_data": None if allegato["additional_data"] is None else Json(allegato["additional_data"])
                })
                id_allegati_da_tenere.append(c.fetchone()["id"])
        if len(id_allegati_da_tenere) > 0:
            c.execute(qc.delete_allegati, {
                "id_allegati_da_tenere": id_allegati_da_tenere,
                "id_doc": id_doc
            })
            c.execute(qc.set_estraibile_flag_su_allegati, {
                "id_doc": id_doc
            })
        else:
            c.execute(qc.delete_allegati_tutti, {
                "id_doc": id_doc
            })
        later = time.time()
        difference_allegati = int(later - now)

        # AGGIORNAMENTO COLLEGI SINDACALI
        now = time.time()
        values_collegio_sindacale = ""
        if json_data['collegi_sindacali'] is not None and len(json_data['collegi_sindacali']) > 0:
            for collegio_sindacale in json_data['collegi_sindacali']:
                if collegio_sindacale is None:
                    raise Exception("il collegio_sindacale nel json è null")
                collegio_sindacale_map = get_and_cache_collegio_sindacale_from_mail(conn, collegio_sindacale, id_azienda)
                if collegio_sindacale_map is None or collegio_sindacale_map == {} or "id" not in collegio_sindacale_map or collegio_sindacale_map["id"] is None:
                    raise Exception("non ho trovato il collegio sindacale " + collegio_sindacale + "in internauta per l'azienda " + str(id_azienda))
                values_collegio_sindacale = values_collegio_sindacale + f"""(
                            {collegio_sindacale_map["id"]}
                        ),"""
        if len(values_collegio_sindacale) > 0:
            # Chiamo la insert and delete
            values_collegio_sindacale = values_collegio_sindacale[:-1]  # rimuovo l'ultima virgola
            c.execute(qc.insert_docs_collegi_sindacali_and_delete_the_others.format(values=values_collegio_sindacale), {
                "id_doc": id_doc
            })
        else:
            # Faccio solo la delete
            c.execute(qc.delete_collegi_sindacali, {
                "id_doc": id_doc
            })
        later = time.time()
        difference_collegi_sindacali = int(later - now)

        # AGGIORNAMENTO INFO ANNULLAMENTO
        c.execute(qc.delete_from_docs_annullati, { "id_doc": id_doc })
        c.execute(qc.delete_nota_doc_annullamento, { "id_doc": id_doc })
        if json_data['annullato'] is not None:
            c.execute(qc.insert_info_annullamento, { 
                "id_doc": id_doc,
                "motivazione": json_data['annullato']["motivazione"],
                "data_annullamento": json_data['annullato']["data_annullamento"] if json_data['annullato']["data_annullamento"] is not None else json_data['data_creazione'],
                "id_persona_annullante": json_data['annullato']["id_persona_annullante"] if json_data['annullato']["id_persona_annullante"] is not None else 1, #se non c'è metto l'utente bds. preferisco tenere il constaint not null sulla colonna
                "tipo_annullamento": json_data['annullato']["stato"],
                "id_esterno_documento_annullamento": json_data['annullato']["id_esterno_documento_annullamento"],
                "id_struttura_annullante": json_data['annullato']["id_struttura_annullante"]
            })

        # AGGIORNAMENTO DEGLI ATTORI
        now = time.time()
        values_attori = ""
        count_attori = 0
        if json_data['attori'] is not None and len(json_data['attori']) > 0:
            for attore in json_data['attori']:
                # idStruttura può essere null solo perché nei vecchi attori non si riescie a fare il match con le strutture internuata
                values_attori = values_attori + f"""(
                    {attore["idPersona"] if attore['idPersona'] is not None else 'null'}, 
                    {attore['idStruttura'] if attore['idStruttura'] is not None else 'null'}, 
                    {"'" + RUOLO_ATTORE[attore['ruolo']] + "'"}, 
                    {attore['ordinale'] if attore['ordinale'] is not None else 'null'},
                    {attore["vedente"]},
                    {attore["sulla_scrivania"]}
                ),"""
                count_attori += 1
        if len(values_attori) > 0:
            # Chiamo la upsert and delete
            values_attori = values_attori[:-1] # rimuovo l'ultima virgola
            # now_query_attori = time.time()
            #disable_enable_trigger_update_doc_detail(conn, "DISABLE")
            #log.info(f"disable del trigger effettuato, ora inserisco {count_attori} attori")
            c.execute(qc.upsert_attori_and_delete_the_others.format(values=values_attori), {
                "id_doc": id_doc
            })
            #disable_enable_trigger_update_doc_detail(conn, "ENABLE")
            # later_query_attori = time.time()
            # difference_query_attori = int(later_query_attori - now_query_attori)
            # if difference_query_attori > 10:
            #     log.info(f"query attori: {difference_query_attori}")
            #     log.info(c.query)
        else:
            # Faccio solo la delete
            c.execute(qc.delete_attori, {
                "id_doc": id_doc
            })
        later = time.time()
        difference_attori = int(later - now)

        #AGGIORNAMENTO DEI FIRMATARI
        now = time.time()
        values_firmatari = ""
        c.execute(qc.delete_firmatari, {
            "id_doc": id_doc
        })
        if json_data['firmatari'] is not None and len(json_data['firmatari']) > 0:
            for firmatario in json_data['firmatari']:
                values_firmatari = values_firmatari + f"""(
                    {firmatario['id_persona'] if firmatario['id_persona'] is not None else 'null'},
                    {"'" + firmatario['tipologia_firma']+ "'::scripta.tipologie_firma" },
                    {"'" + firmatario['ts_firma'] + "'" if firmatario['ts_firma'] is not None else 'null'},
                    {"'" + firmatario['stato'] + "'::scripta.stati_firmatario" }
                    ),"""

        if len(values_firmatari) > 0:
            values_firmatari = values_firmatari[:-1] # rimuovo l'ultima virgola
            c.execute(qc.insert_firmatari.format(values=values_firmatari), {
                "id_doc": id_doc
            })
        later = time.time()
        difference_firmatari = int(later - now)

        # AGGIORNAMENTO FIRMATARI ALLEGATI
        now = time.time()
        values_firmatari_allegati = ""
        c.execute(qc.delete_firmatari_allegati, {
                "id_doc": id_doc
            })
        if json_data['firmatari_allegati'] is not None and len(json_data['firmatari_allegati']) > 0:
            for firmatario_allegato in json_data['firmatari_allegati']:
                values_firmatari_allegati = values_firmatari_allegati + f"""(
                            {"'" + firmatario_allegato['id_allegato'] + "'"},
                            { firmatario_allegato['id_persona_attore']   if firmatario_allegato['id_persona_attore'] is not None else 'null'},
                            {"'" + firmatario_allegato['tipologia_firma']+ "'::scripta.tipologie_firma"},
                            {firmatario_allegato['firmato']},
                            {"'" + firmatario_allegato['ts_firma'] + "'" if firmatario_allegato['ts_firma'] is not None else 'null'},
                            {"'" + firmatario_allegato['dettaglio_firmato']+ "'::scripta.tipi_dettagli_allegati"}
                            ),"""

        if len(values_firmatari_allegati) > 0:
            values_firmatari_allegati = values_firmatari_allegati[:-1]  # rimuovo l'ultima virgola
            c.execute(qc.insert_firmatari_allegati.format(values=values_firmatari_allegati), {
                "id_doc": id_doc
            })
        later = time.time()
        difference_firmatari_allegati = int(later - now)

        # AGGIORNAMENTO DEL DOC DETAILS -  DO L'INCARICO AL MASTERJOBS
        c.execute(qc.insert_job_upsert_doc_detail, {
            "id_doc": id_doc
        })

        # AGGIORNAMENTO DELLE PERSONE VEDENTI - DO L'INCARICO AL MASTERJOBS
        now = time.time()
        c.execute(qc.insert_job_calcola_persone_vedenti, {
            "id_doc": id_doc
        })
        later = time.time()
        difference_persone_vedenti = int(later - now)

        # DOCUMENTO AGGIORNATO. COMMITTO
        conn.commit()
        log.info(f"upsert_doc_list_data eseguita con successo per documento con guid: {json_data['guid_documento']}")
        # log.info("%s secondi upsert, %s secondi pers.vedenti, %s secondi allegati, %s secondi difference_attori, %s secondi difference_collegi_sindacali. %s secondi difference_firmatari, %s secondi difference_firmatari_allegati" 
        #          % (str(difference_upsert), str(difference_persone_vedenti), str(difference_allegati), str(difference_attori), str(difference_collegi_sindacali), str(difference_firmatari), str(difference_firmatari_allegati)))
        log.info(f"""
            Tempi di elaborazione:
            {difference_upsert} secondi upsert
            {difference_persone_vedenti} secondi persone vedenti
            {difference_allegati} secondi allegati
            {difference_attori} secondi attori
            {difference_collegi_sindacali} secondi collegi sindacali
            {difference_firmatari} secondi firmatari
            {difference_firmatari_allegati} secondi firmatari allegati
        """)
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


def get_and_cache_collegio_sindacale_from_mail(conn, email, id_azienda):
    global map_collegi_sindcali
    key_to_find = email + "__" + str(id_azienda)
    if key_to_find not in map_collegi_sindcali:
        c = conn.cursor(cursor_factory=psycopg2.extras.DictCursor)
        c.execute(qc.get_collegi_sindacali, {'email': email, 'id_azienda': id_azienda})
        map_collegi_sindcali = c.fetchone()["collegi_sindacali_map"]
    if key_to_find in map_collegi_sindcali:
        return map_collegi_sindcali[key_to_find]
    else:
        return None


def upsert_related(json_data, conn, id_azienda):
    c = conn.cursor(cursor_factory=psycopg2.extras.DictCursor)

    # PRENDO L'ID DEL DOC
    c.execute(qc.select_id_doc_from_id_esterno, {
        "guid_doc": json_data["guid_documento"],
        "id_azienda": id_azienda
    })
    if c.rowcount == 0:
        log.info("Il documento non esiste, non devo fare nulla")
        return
    id_doc = c.fetchone()["id"]

    #disable_enable_trigger_update_doc_detail(conn, "DISABLE")

    # AGGIORNO LA MESSAGES_DOCS
    if "id_message_shpeck" in json_data:
        if json_data["id_message_shpeck"] is None:
            c.execute(qc.delete_messages_docs, {
                "id_message": json_data['id_message_shpeck'],
                "id_doc": id_doc
            })
        elif json_data["tipologia"] == "PROTOCOLLO_IN_ENTRATA":
            c.execute(qc.insert_messages_docs_pe, {
                "id_message": json_data['id_message_shpeck'],
                "id_doc": id_doc
            })
        elif json_data["tipologia"] == "PROTOCOLLO_IN_USCITA":
            for message in json_data['id_message_shpeck']:
                c.execute(qc.insert_messages_docs_pu, {
                    "id_message": message,
                    "id_doc": id_doc
                })

    # AGGIORNO I RELATED
    values_related = ""
    idContattoMembro_idContattoGruppo = {}
    idContatto_idEsterno = {}
    if json_data["related"] is not None and len(json_data['related']) > 0:
        
        for related in json_data['related']:
            # idStruttura può essere null solo perché nei vecchi attori non si riescie a fare il match con le strutture internuata
            #faccio escape dei caratteri speciali e replace degli apostrofi, aggiungo il replace dei percento perchè psycopg è psico e sennò fa confusione coi parametri,
            #gli altri caratteri non danno problemi
            descrizione = ''
            indirizzo = ''
            id_contatto_gruppo = ''
            if related['descrizione'] is not None:
                descrizione = related['descrizione'].replace("'", "''").replace("%", "%%")
            if related['indirizzo'] is not None:
                indirizzo = related['indirizzo'].replace("'", "''").replace("%", "%%")
            if related['id_gruppo'] != '' and related['id_gruppo'] is not None:
                id_contatto_gruppo = related['id_gruppo'].replace("g_", "")
            if id_contatto_gruppo != '' and id_contatto_gruppo is not None:
                idContattoMembro_idContattoGruppo[str(related['id_contatto'])] = id_contatto_gruppo
            if related['id_esterno'] is not None:
               idContatto_idEsterno[str(related['id_contatto'])] = related['id_esterno']
            values_related = values_related + f"""(
                        {related['id_contatto'] if related['id_contatto'] is not None else 'null'},
                        {related['id_persona_inserente'] if related['id_persona_inserente'] is not None else 1}, 
                        {"'" + related['tipo'] + "'"},
                        {"'" + related['origine'] + "'"},
                        {"'" + descrizione + "'" if descrizione is not None and descrizione  != '' else "'" + indirizzo + "'"},
                        {"'" + related['data_inserimento'] + "'"},
                        {"'" + str(related['id_esterno']) + "'" if related['id_esterno'] is not None else 'null'}, 
                        {related['is_gruppo']} 
                    ),"""
    # log.info(f"QUESTI SONO I RELATED CHE VOGLIO INSERIRE: {values_related}")
    if len(values_related) > 0:

        # Chiamo la upsert and delete
        values_related = values_related[:-1]  # rimuovo l'ultima virgola
        #string = qc.upsert_related_and_delete_the_others.format(values=values_related)
        #log.info(f"mi da fastidio sto coso: {string}")
        c.execute(qc.upsert_related_and_delete_the_others.format(values=values_related), {
            "id_doc": id_doc
        })

        # Aggiorno anche id_gruppo
        # calcolo la condizione (CASE WHEN THEN) dinamica da mettere nella query per l'update
        case_conditions = []
        for idContattoFiglio, idContattoGruppo in idContattoMembro_idContattoGruppo.items():
            case_conditions.append(f"WHEN '{idContatto_idEsterno[idContattoFiglio]}' THEN '{idContatto_idEsterno[idContattoGruppo]}'")
        case_statement = " ".join(case_conditions)

        # eseguo query di update
        print(qc.upsert_related_real_id_gruppo.format(case_statement=case_statement), {
            "id_doc": id_doc
        })
        c.execute(qc.upsert_related_real_id_gruppo.format(case_statement=case_statement), {
            "id_doc": id_doc
        })

    else:
        # Faccio solo la delete
        c.execute(qc.delete_related, {
            "id_doc": id_doc
        })

    # INSERIMENTO SPEDIZIONI
    #if json_data['id_message_shpeck'] is not None:
    if json_data["related"] is not None and len(json_data['related']) > 0:
        for related in json_data['related']:

            if related['mezzo'] == 'Email' or related['mezzo'] is None:
                related['mezzo'] = 'Mail'
            if related['mezzo'] == 'Posta Ordinaria' or related['mezzo'] == 'P. Ordin.' or related['mezzo'] == 'Posta' or related['mezzo'] == 'Corriere':
                related['mezzo'] = 'Posta ordinaria'
            if related['mezzo'] == 'A Mano':
                related['mezzo'] = 'A mano'
            if related['mezzo'] == 'Racc' or related['mezzo'] == 'Racc. A/R':
                related['mezzo'] = 'Raccomandata'
            if related['mezzo'] == 'Tel' or related['mezzo'] == 'telefono':
                related['mezzo'] = 'Telefono'
            if related['mezzo'] == 'PEC':
                related['mezzo'] = 'Pec'
            if related['mezzo'] == 'Gruppo':
                related['mezzo'] = 'Vario'

            log.info(f"questo è il mezzo che sto cercando: {related['mezzo']}" )
            c.execute(qc.seleziona_id_mezzo, {
                "mezzo": related['mezzo']})
            id_mezzo = c.fetchone()["id"]
            if related['is_gruppo'] == False : #se non è un gruppo allora inserirsco la spedizione
                if json_data["tipologia"] == "PROTOCOLLO_IN_ENTRATA" or json_data["tipologia"] == "DELIBERA" or json_data["tipologia"] == "DETERMINA":
                    c.execute(qc.upsert_spedizione, {
                        "guid_doc": json_data["guid_documento"],
                        "id_message": json_data['id_message_shpeck'],
                        "id_mezzo": id_mezzo,
                        "indirizzo": related["indirizzo"],
                        "id_esterno": related["id_esterno"]
                    })
                else:
                    if "id_spedizione_pec" in related:
                        c.execute(qc.upsert_spedizione, {
                            "guid_doc": json_data["guid_documento"],
                            "id_message": related["id_spedizione_pec"],
                            "id_mezzo": id_mezzo,
                            "indirizzo": related["indirizzo"],
                            "id_esterno": related["id_esterno"]
                        })
                    else:
                        c.execute(qc.upsert_spedizione, {
                            "guid_doc": json_data["guid_documento"],
                            "id_message": None,
                            "id_mezzo": id_mezzo,
                            "indirizzo": related["indirizzo"],
                            "id_esterno": related["id_esterno"]
                        })

    else:
        c.execute(qc.delete_spedizione, {
            "guid_doc": json_data["guid_documento"]
        })

    #disable_enable_trigger_update_doc_detail(conn, "ENABLE")
    conn.commit()
