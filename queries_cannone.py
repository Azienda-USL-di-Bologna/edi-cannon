# -*- coding: utf-8 -*-
insert_doc = """
INSERT INTO scripta.docs_list
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
                        persone_vedenti,
                        id_mezzo_ricezione,
                        id_strutture_firmatari,
                        sulla_scrivania_di
            )
            VALUES
            (
                        %(id_azienda)s,
                        %(guid_documento)s,
                        %(tipologia)s, --  id_azienda, guid_documento, tipologia
                        %(open_command)s,
                        %(command_type)s, --  open_command,command_type
                        (select id from baborg.persone where codice_fiscale = %(id_persona_responsabile_procedimento)s),
                        %(id_persona_redattrice)s,
                        %(id_struttura_registrazione)s, --  id_persona_responsabile_procedimento, id_persona_redattrice, id_struttura_registrazione
                        %(numero_proposta)s,
                        %(anno_proposta)s,
                        %(numero_registrazione)s,
                        %(anno_registrazione)s, --  numero_proposta, anno_proposta, numero_registrazione, anno_registrazione
                        %(data_creazione)s,
                        %(data_registrazione)s,
                        %(data_pubblicazione)s, --  data_creazione, data_registrazione, data_pubblicazione,
                        %(oggetto)s,
                        %(fascicolazioni)s::jsonb,
                        %(classificazioni)s, --  oggetto, fascicolazioni, classificazioni,
                        %(firmatari)s,
                        %(destinatari)s,
                        %(mittente)s, --  firmatari, destinatari,  mittente,
                        %(stato)s,
                        %(visibilita_limitata)s,
                        %(riservato)s,
                        %(annullato)s, --  stato, visibilita_limitata, riservato ,annullato,
                        %(protocollo_esterno)s,
                        %(mail_collegio)s,
                        %(stato_ufficio_atti)s,    --  protocollo_esterno,mail_collegio, stato_ufficio_atti,
                        %(data_inserimento_riga)s, --  data_inserimento_riga,
                        %(persone_vedenti)s,
                        (select id from scripta.mezzi where descrizione = %(id_mezzo_ricezione)s),
                        %(id_strutture_firmatari)s,
                        %(sulla_scrivania_di)s --  persone_vedenti, id_mezzo_ricezione, id_strutture_firmatari, sulla_scrivania_di
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
       numero_proposta = excluded.numero_proposta ,
       anno_proposta = excluded.anno_proposta ,
       numero_registrazione = excluded.numero_registrazione ,
       anno_registrazione = excluded.anno_registrazione ,
       data_creazione = excluded.data_creazione ,
       data_registrazione = excluded.data_registrazione ,
       data_pubblicazione = excluded.data_pubblicazione,
       oggetto = excluded.oggetto ,
       fascicolazioni = excluded.fascicolazioni ,
       classificazioni = excluded.classificazioni ,
       firmatari = excluded.firmatari ,
       destinatari = excluded.destinatari ,
       mittente = excluded.mittente ,
       stato = excluded.stato ,
       visibilita_limitata = excluded.visibilita_limitata ,
       riservato = excluded.riservato ,
       annullato = excluded.annullato ,
       protocollo_esterno = excluded.protocollo_esterno ,
       mail_collegio = excluded.mail_collegio ,
       stato_ufficio_atti = excluded.stato_ufficio_atti,
       persone_vedenti = excluded.persone_vedenti ,
       id_mezzo_ricezione = excluded.id_mezzo_ricezione ,
       id_strutture_firmatari = excluded.id_strutture_firmatari ,
       sulla_scrivania_di = excluded.sulla_scrivania_di
"""