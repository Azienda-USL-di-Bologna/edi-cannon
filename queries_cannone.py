# -*- coding: utf-8 -*-
insert_doc = """
INSERT INTO scripta.docs_details
            (
                        id_azienda,
                        guid_documento,
                        tipologia,
                        open_command,
                        command_type,
                        id_persona_responsabile_procedimento,
                        id_persona_redattrice,
                        id_struttura_registrazione,
                        numero_proposta,
                        anno_proposta,
                        numero_registrazione,
                        anno_registrazione,
                        data_creazione,
                        data_registrazione,
                        data_pubblicazione,
                        oggetto,
                        fascicolazioni,
                        classificazioni,
                        firmatari,
                        destinatari,
                        mittente,
                        stato,
                        visibilita_limitata,
                        riservato ,
                        annullato,
                        protocollo_esterno,
                        mail_collegio,
                        stato_ufficio_atti,
                        data_inserimento_riga,
                        --persone_vedenti,
                        id_mezzo_ricezione,
                        id_strutture_segreteria,
                        sulla_scrivania_di,
                        version,
                        id_applicazione
            )
            VALUES
            (
                        (select id from baborg.aziende where codice = %(codice_azienda)s),
                        %(guid_documento)s,
                        %(tipologia)s, 
                        %(open_command)s,
                        %(command_type)s,
                        %(id_persona_responsabile_procedimento)s,
                        %(id_persona_redattrice)s,
                        %(id_struttura_registrazione)s,
                        %(numero_proposta)s,
                        %(anno_proposta)s,
                        %(numero_registrazione)s,
                        %(anno_registrazione)s,
                        %(data_creazione)s,
                        %(data_registrazione)s,
                        %(data_pubblicazione)s,
                        %(oggetto)s,
                        %(fascicolazioni)s::jsonb,
                        %(classificazioni)s,
                        %(firmatari)s,
                        %(destinatari)s,
                        %(mittente)s,
                        %(stato)s,
                        %(visibilita_limitata)s,
                        %(riservato)s,
                        %(annullato)s, 
                        %(protocollo_esterno)s,
                        %(mail_collegio)s,
                        %(stato_ufficio_atti)s,
                        %(data_inserimento_riga)s,
                        --%(persone_vedenti)s,
                        (select id from scripta.mezzi where descrizione = %(id_mezzo_ricezione)s),
                        %(id_strutture_segreteria)s,
                        %(sulla_scrivania_di)s,
                        %(version)s,
                        %(id_applicazione)s 
            )
ON conflict
            (
                        guid_documento
            )
            do UPDATE
set    open_command = excluded.open_command,
       command_type = excluded.command_type,
       id_persona_responsabile_procedimento = excluded.id_persona_responsabile_procedimento,
       id_persona_redattrice = excluded.id_persona_redattrice,
       id_struttura_registrazione = excluded.id_struttura_registrazione,
       numero_proposta = excluded.numero_proposta,
       anno_proposta = excluded.anno_proposta,
       numero_registrazione = excluded.numero_registrazione,
       anno_registrazione = excluded.anno_registrazione,
       data_creazione = excluded.data_creazione,
       data_registrazione = excluded.data_registrazione,
       data_pubblicazione = excluded.data_pubblicazione,
       oggetto = excluded.oggetto,
       fascicolazioni = excluded.fascicolazioni,
       classificazioni = excluded.classificazioni,
       firmatari = excluded.firmatari,
       destinatari = excluded.destinatari,
       mittente = excluded.mittente,
       stato = excluded.stato,
       visibilita_limitata = excluded.visibilita_limitata,
       riservato = excluded.riservato,
       annullato = excluded.annullato,
       protocollo_esterno = excluded.protocollo_esterno ,
       mail_collegio = excluded.mail_collegio,
       stato_ufficio_atti = excluded.stato_ufficio_atti,
       --persone_vedenti = excluded.persone_vedenti,
       id_mezzo_ricezione = excluded.id_mezzo_ricezione,
       id_strutture_segreteria = excluded.id_strutture_segreteria,
       sulla_scrivania_di = excluded.sulla_scrivania_di,
       version = excluded.version ,
       id_applicazione = excluded.id_applicazione
"""
delete_persone_vedenti = """
    DELETE FROM scripta.persone_vedenti pv
    USING scripta.docs_details dd
    WHERE dd.id_doc_detail = dd.id
    AND dd.guid_documento = %(guid_documento)s
"""
insert_persone_vedenti = """
    INSERT INTO scripta.persone_vedenti (id_doc_detail, id_persona, mio_documento, piena_visibilita, modalita_apertura) 
    VALUES((SELECT id FROM scripta.docs_details WHERE guid_documento = %(guid_documento)s), %(id_persona)s, %(mio_documento)s, %(piena_visibilita)s, %(modalita_apertura)s);
"""