import logging
log = logging.getLogger("cannoneggiamento_aziendale")

def get_pico_document_by_guid(conn, guid):
    log = logging.getLogger("cannoneggiamento_aziendale")
    qUery = "select * from esportazioni.get_procton_document_data_by_guid(%s)"
    try:
        c = conn.cursor()
        c.execute(qUery, (guid,))
        log.info(f"get_pico_document_by_guid eseguita con successo per guid: {guid}")
        return c.fetchone()
    except Exception as ex:
        log.error(f"get_pico_document_by_guid fallita per guid: {guid}")
        log.error(ex)
        raise ex


def get_dete_document_by_guid(conn, guid):
    log = logging.getLogger("cannoneggiamento_aziendale")
    qUery = "select * from esportazioni.get_dete_document_data_by_guid(%s)"
    try:
        c = conn.cursor()
        c.execute(qUery, (guid,))
        log.info(f"get_dete_document_by_guid eseguita con successo per guid: {guid}")
        return c.fetchone()
    except Exception as ex:
        log.error(f"get_dete_document_by_guid fallita per guid: {guid}")
        log.error(ex)
        raise ex


def get_deli_document_by_guid(conn, guid):
    log = logging.getLogger("cannoneggiamento_aziendale")
    qUery = "select * from esportazioni.get_deli_document_data_by_guid(%s)"
    try:
        c = conn.cursor()
        c.execute(qUery, (guid,))
        log.info(f"get_deli_document_by_guid eseguita con successo per guid: {guid}")
        return c.fetchone()
    except Exception as ex:
        log.error(f"get_deli_document_by_guid fallita per guid: {guid}")
        log.error(ex)
        raise ex
