# -*- coding: utf-8 -*-
insert_doc = """
    with insert_to_docs as (
        INSERT INTO scripta.docs  (           
            oggetto,
            id_persona_creazione,
            data_creazione,
            id_azienda,
            tipologia,
            visibilita,
            id_esterno
        ) VALUES (
            %(oggetto)s,
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
        ) ON conflict (id_esterno)
        do UPDATE
        set oggetto = excluded.oggetto,
            id_persona_creazione = excluded.id_persona_creazione,
            tipologia = excluded.tipologia
        RETURNING id, data_creazione
    )         
    INSERT INTO scripta.docs_details (
        id,
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
    ) VALUES (
        (select id from insert_to_docs),
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
    ) ON conflict (guid_documento, id_azienda, data_creazione)
    DO UPDATE
    SET open_command = excluded.open_command,
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
    RETURNING id
"""
upsert_persone_vedenti_and_delete_the_others = """
    WITH data_creazione AS (
        SELECT dd.data_creazione 
        FROM scripta.docs dd
        WHERE dd.id_esterno = %(guid_documento)s
        AND dd.id_azienda = %(id_azienda)s
    ),
    id_da_tenere AS (
        INSERT INTO scripta.persone_vedenti (
            id_doc_detail, id_persona, mio_documento, piena_visibilita, 
            modalita_apertura, data_creazione, data_registrazione, id_azienda
        ) 
        SELECT %(id_doc)s, id_persona, mio_documento, piena_visibilita, 
            modalita_apertura, ( SELECT data_creazione FROM data_creazione ), %(data_registrazione)s, %(id_azienda)s 
        FROM (
        VALUES  
            {values}
        ) AS t (id_persona, mio_documento, piena_visibilita, modalita_apertura)
        ON CONFLICT (id_doc_detail, id_persona, data_creazione, id_azienda) DO UPDATE 
        SET mio_documento = EXCLUDED.mio_documento,
            piena_visibilita = EXCLUDED.piena_visibilita,
            modalita_apertura = EXCLUDED.modalita_apertura
        RETURNING id
    )
    DELETE FROM scripta.persone_vedenti 
    WHERE id_azienda = %(id_azienda)s
    AND data_creazione = ( SELECT data_creazione FROM data_creazione )
    AND id_doc_detail = %(id_doc)s
    AND id NOT IN (SELECT id FROM id_da_tenere)
"""
delete_persone_vedenti = """
    DELETE FROM scripta.persone_vedenti pv
    USING scripta.docs dd
    WHERE pv.id_doc_detail = dd.id
    AND dd.id_esterno = %(guid_documento)s
    AND pv.id_azienda = %(id_azienda)s
    AND dd.id_azienda = %(id_azienda)s
    AND pv.data_creazione = dd.data_creazione
"""
insert_persone_vedenti = """
    INSERT INTO scripta.persone_vedenti 
        (id_doc_detail, id_persona, mio_documento, piena_visibilita, 
        modalita_apertura, data_creazione, data_registrazione, id_azienda) 
    VALUES (
        (   SELECT dd.id 
            FROM scripta.docs dd
            WHERE dd.id_esterno = %(guid_documento)s
            AND dd.id_azienda = %(id_azienda)s
        ), 
        %(id_persona)s, 
        %(mio_documento)s, 
        %(piena_visibilita)s, 
        %(modalita_apertura)s,
        (   SELECT dd.data_creazione 
            FROM scripta.docs dd
            WHERE dd.id_esterno = %(guid_documento)s
            AND dd.id_azienda = %(id_azienda)s
        ),
        %(data_registrazione)s,
        %(id_azienda)s
    )
"""
delete_doc = """
    DELETE FROM scripta.docs 
    WHERE id_esterno = %(guid_documento)s
    AND id_azienda = %(id_azienda)s
"""
insert_allegati_doc = """
    INSERT INTO scripta.allegati (
        nome, tipo, principale, firmato, 
        ordinale, id_doc, id_allegato_padre, data_inserimento, 
        dettagli, id_esterno, sottotipo, additional_data
    ) VALUES ( 
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
        %(dettagli)s,
        %(id_esterno)s,
        %(sottotipo)s,
        %(additional_data)s
        )
    ON CONFLICT (id_esterno, tipo) DO UPDATE
    SET nome = excluded.nome,
        tipo = excluded.tipo,
        principale = excluded.principale,
        firmato = excluded.firmato,
        ordinale = excluded.ordinale,
        id_doc = excluded.id_doc,
        id_allegato_padre = excluded.id_allegato_padre,
        data_inserimento = excluded.data_inserimento,
        dettagli = excluded.dettagli,
        sottotipo = excluded.sottotipo,
        additional_data = excluded.additional_data
"""
query_minio = """
    SELECT jsonb_object_agg(mongo_uuid, jsonb_build_object(
        'idRepository', file_id, 
        'nome', filename, 
        'dimensioneByte', size, 
        'hashMd5', md5
        )
    ) AS res
    FROM repo.files
    WHERE mongo_uuid = ANY(%(mongo_uuids)s)
"""
upsert_attori_and_delete_the_others = """
    WITH id_da_tenere AS (
        INSERT INTO scripta.attori_docs (
            id_doc, id_persona, id_struttura, ruolo, 
            sulla_scrivania, ordinale, data_inserimento_riga
        ) 
        SELECT %(id_doc)s, id_persona, id_struttura, ruolo::scripta.ruolo_attore_doc, 
            FALSE, ordinale::integer, now() 
        FROM (
        VALUES  
            {values}
        ) AS t (id_persona, id_struttura, ruolo, ordinale)
        ON CONFLICT (id_doc, id_persona, id_struttura, ruolo) DO UPDATE 
        SET sulla_scrivania = EXCLUDED.sulla_scrivania,
            ordinale = EXCLUDED.ordinale
        RETURNING id
    )
    DELETE FROM scripta.attori_docs 
    WHERE id_doc = %(id_doc)s
    AND id NOT IN (SELECT id FROM id_da_tenere)
"""
delete_attori = """
    DELETE FROM scripta.attori_docs 
    WHERE id_doc = %(id_doc)s
"""