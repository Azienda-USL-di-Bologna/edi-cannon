# -*- coding: utf-8 -*-
insert_doc = """
with insert_to_docs as (
    INSERT INTO scripta.docs 
                (           oggetto,
                            id_persona_creazione,
                            data_creazione,
                            id_azienda,
                            tipologia,
                            visibilita,
                            id_esterno
                )
                VALUES
            (               %(oggetto)s,
                            %(id_persona_redattrice)s,
                            %(data_creazione)s,
                            %(id_azienda)s,
                            %(tipologia)s,
                            CASE
                               WHEN %(riservato)s = true
                                    THEN 'RISERVATO'::scripta.visibilita_doc
                               WHEN %(visibilita_limitata)s  = true
                                   THEN 'LIMITATA'::scripta.visibilita_doc
                               else 'NORMALE'::scripta.visibilita_doc
                            END,
                            %(guid_documento)s
                )
                ON conflict
                (
                            id_esterno
                )
                do UPDATE
    set    oggetto = excluded.oggetto,
           id_persona_creazione = excluded.id_persona_creazione,
           tipologia = excluded.tipologia
   RETURNING id, data_creazione
)         
INSERT INTO scripta.docs_details
            (           id,
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
                        id_mezzo_ricezione,
                        id_strutture_segreteria,
                        sulla_scrivania_di,
                        version,
                        id_applicazione,
                        conservazione
            )
            VALUES
            (           (select id from insert_to_docs),
                        %(id_azienda)s,
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
                        (select data_creazione from insert_to_docs),
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
                        (select id from scripta.mezzi where descrizione = %(id_mezzo_ricezione)s),
                        %(id_strutture_segreteria)s,
                        %(sulla_scrivania_di)s,
                        %(version)s,
                        %(id_applicazione)s,
                        %(conservazione)s
            )
ON conflict
            (
                        guid_documento, id_azienda, data_creazione
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
       id_mezzo_ricezione = excluded.id_mezzo_ricezione,
       id_strutture_segreteria = excluded.id_strutture_segreteria,
       sulla_scrivania_di = excluded.sulla_scrivania_di,
       version = excluded.version ,
       id_applicazione = excluded.id_applicazione,
       conservazione = excluded.conservazione
returning id
"""
delete_persone_vedenti = """
    DELETE FROM scripta.persone_vedenti pv
    USING scripta.docs_details dd
    WHERE pv.id_doc_detail = dd.id
    AND dd.guid_documento = %(guid_documento)s
    AND pv.id_azienda = %(id_azienda)s
    AND dd.id_azienda = %(id_azienda)s
    AND pv.data_creazione = %(data_creazione)s
    AND dd.data_creazione = %(data_creazione)s
"""
insert_persone_vedenti = """
    INSERT INTO scripta.persone_vedenti 
        (id_doc_detail, id_persona, mio_documento, piena_visibilita, 
        modalita_apertura, data_creazione, data_registrazione, id_azienda) 
    VALUES(
        (   SELECT dd.id 
            FROM scripta.docs_details dd
            WHERE dd.guid_documento = %(guid_documento)s
            AND dd.id_azienda = %(id_azienda)s
            AND dd.data_creazione = %(data_creazione)s
        ), 
        %(id_persona)s, 
        %(mio_documento)s, 
        %(piena_visibilita)s, 
        %(modalita_apertura)s,
        %(data_creazione)s,
        %(data_registrazione)s,
        %(id_azienda)s
    )
"""
delete_doc = """
    delete from scripta.docs 
    where id_esterno = %(guid_documento)s
"""

insert_allegati_doc = """
    INSERT INTO scripta.allegati
        (nome, tipo, principale, firmato, ordinale, id_doc, id_allegato_padre, data_inserimento, version, dettagli, id_esterno)
    VALUES 
        ( 
        %(nome)s,
        %(tipo)s,
        CASE
            when %(principale)s != 0 then true::boolean
            else false::boolean
        END,
        %(firmato)s,
        %(ordinale)s,
        (   SELECT id
            FROM scripta.docs
            WHERE id_esterno = %(guid_documento)s
        ),
        (   SELECT a.id 
            FROM scripta.allegati a 
            WHERE a.id_esterno = %(id_allegato_padre)s
        ),
        %(data_inserimento)s,
        now(),
        %(dettagli)s,
        %(id_esterno)s
        )
        ON conflict
            (
                        id_esterno, tipo
            )
            do UPDATE
set    nome = excluded.nome,
       tipo = excluded.tipo,
       principale = excluded.principale,
       firmato = excluded.firmato,
       ordinale = excluded.ordinale,
       id_doc = excluded.id_doc,
       id_allegato_padre = excluded.id_allegato_padre,
       data_inserimento = excluded.data_inserimento,
       version = excluded.version,
       dettagli = excluded.dettagli
"""

query_minio = """
SELECT jsonb_object_agg(mongo_uuid, jsonb_build_object(
    'idRepository', file_id, 
    'nome', filename, 
    'dimensioneByte', size, 
    'hashMd5', md5
    )
) as res
FROM repo.files
WHERE mongo_uuid =ANY(
    %(mongo_uuids)s
)
"""