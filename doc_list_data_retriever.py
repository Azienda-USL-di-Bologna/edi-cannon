
import argo_data_retriever as adr

def get_edi_data_from_pico(conn, guid):
    res = adr.get_pico_document_by_guid(conn, guid)
    print(str(res[0]))
    return res[0]