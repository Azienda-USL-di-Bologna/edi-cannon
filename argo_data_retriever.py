import logging
import psycopg2.extras
import time

log = logging.getLogger("cannoneggiamento_aziendale")

mappa_stored_procedure = {
    "pico_pe": "get_procton_pe_document_data_by_guid",
    "pico_pu": "get_procton_pu_document_data_by_guid",
    "dete": "get_dete_document_data_by_guid",
    "deli": "get_deli_document_data_by_guid",
    "RGPICO": "get_registri_giornalieri_by_id",
    "RGDETE": "get_registri_giornalieri_by_id",
    "RGDELI": "get_registri_giornalieri_by_id"
}

"""
    Torna un mega json che rappresenta il documento associato al guid passato.
"""
def get_document_by_guid(conn, guid, tipo_documento):
    select = "select * from esportazioni.%s" % mappa_stored_procedure[tipo_documento] + "(%(guid)s)"
    try:
        c = conn.cursor(cursor_factory=psycopg2.extras.DictCursor)
        now = time.time()
        c.execute(select, {"guid": guid})
        later = time.time()
        difference = int(later - now)
        log.info(f"get_document_by_guid eseguita con successo per {tipo_documento} guid: {guid} in %s secondi" % str(difference))
        return c.fetchone()[0]
    except Exception as ex:
        log.error(f"get_document_by_guid fallita per {tipo_documento} guid: {guid}")
        log.error(ex)
        raise ex


"""
    Torno il nome del fascicolo
"""
def get_nome_fascicolo(conn, id_fascicolo):
    try:
        c = conn.cursor(cursor_factory=psycopg2.extras.DictCursor)
        select = "select nome_fascicolo from gd.fascicoligd where id_fascicolo = %(id_fascicolo)s"
        c.execute(select, {'id_fascicolo': id_fascicolo})
        result = c.fetchone()
        log.info("get_nome_fascicolo eseguita con successo, nome: %s" % result['nome_fascicolo'])
        return result['nome_fascicolo']
    except Exception as ex:
        log.error("get_nome_fascicolo fallita per fascicolo: %s" % id_fascicolo)
        raise ex
