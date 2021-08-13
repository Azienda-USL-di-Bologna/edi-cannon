
def get_pico_document_by_guid(conn, guid):
    print(guid)
    qUery = "select * from esportazioni.get_procton_document_data_by_guid(%s)"
    c = conn.cursor()
    c.execute(qUery, (guid,))
    return c.fetchone()

def get_dete_document_by_guid(conn, guid):
    print(guid)
    qUery = "select * from esportazioni.get_dete_document_data_by_guid(%s)"
    c = conn.cursor()
    c.execute(qUery, (guid,))
    return c.fetchone()
