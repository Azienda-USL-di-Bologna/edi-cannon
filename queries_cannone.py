# -*- coding: utf-8 -*-
update_doc_by_id = """
    UPDATE scripta.docs d
    SET oggetto = %(oggetto)s,
        testo = %(testo)s,
        data_registrazione = %(data_registrazione)s,
        id_persona_creazione = %(id_persona_redattrice)s,
        tipologia = %(tipologia)s,
        version = %(version)s,
        additional_data = %(additional_data)s,
        id_esterno = %(guid_documento)s,
        stato = %(stato)s,
        id_struttura_registrante = %(id_struttura_registrazione)s,
        visibilita = (CASE
                WHEN %(riservato)s is true
                    THEN 'RISERVATO'::scripta.visibilita_doc
                WHEN %(visibilita_limitata)s  is true
                    THEN 'LIMITATA'::scripta.visibilita_doc
                else 'NORMALE'::scripta.visibilita_doc
            END)
    WHERE d.id = %(id_doc)s
    RETURNING id, data_creazione
"""
insert_doc = """
        INSERT INTO scripta.docs  (           
            oggetto,
            testo,
            id_persona_creazione,
            data_creazione,
            id_azienda,
            tipologia,
            visibilita,
            id_esterno,
            id_pec_mittente,
            version,
            additional_data,
            stato,
            data_registrazione,
            id_struttura_registrante
            ) VALUES (
            %(oggetto)s,
            %(testo)s,
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
            %(guid_documento)s,
            %(id_pec_mittente)s,
            %(version)s,
            %(additional_data)s,
            %(stato)s,
            %(data_registrazione)s,
            %(id_struttura_registrazione)s
        ) ON conflict (id_azienda, id_esterno)
        do UPDATE
        set oggetto = excluded.oggetto,
            testo = excluded.testo,
            id_persona_creazione = excluded.id_persona_creazione,
            tipologia = excluded.tipologia,
            id_pec_mittente = excluded.id_pec_mittente,
            version = excluded.version,
            additional_data = excluded.additional_data,
            stato = excluded.stato,
            id_struttura_registrante = excluded.id_struttura_registrante,
            data_registrazione = excluded.data_registrazione
        RETURNING id, data_creazione
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
            modalita_apertura, data_creazione, data_registrazione, id_azienda, version
        ) 
        SELECT %(id_doc)s, id_persona, mio_documento, piena_visibilita, 
            modalita_apertura, ( SELECT data_creazione FROM data_creazione ), %(data_registrazione)s, %(id_azienda)s, now()
        FROM (
        VALUES  
            {values}
        ) AS t (id_persona, mio_documento, piena_visibilita, modalita_apertura)
        ON CONFLICT (id_doc_detail, id_persona, data_creazione, id_azienda) DO UPDATE 
        SET mio_documento = EXCLUDED.mio_documento,
            piena_visibilita = EXCLUDED.piena_visibilita,
            modalita_apertura = EXCLUDED.modalita_apertura,
            version = EXCLUDED.version
        RETURNING id
    ),
    altri_id_da_tenere AS (
        INSERT INTO scripta.persone_vedenti (
            id_doc_detail, id_persona, mio_documento, piena_visibilita, 
            data_creazione, data_registrazione, id_azienda, version
        ) 
        SELECT DISTINCT ON (pa.id_persona) %(id_doc)s, pa.id_persona, FALSE, TRUE, 
            d.data_creazione, %(data_registrazione)s, %(id_azienda)s, now()
        FROM scripta.docs d 
		JOIN scripta.archivi_docs ad ON ad.id_doc = d.id 
		JOIN scripta.archivi a ON a.id = ad.id_archivio 
		JOIN scripta.permessi_archivi pa ON pa.id_archivio_detail = a.id AND pa.data_creazione = a.data_creazione AND pa.id_azienda = a.id_azienda 
		WHERE d.id = %(id_doc)s
		AND pa.BIT > 1
        ON CONFLICT (id_doc_detail, id_persona, data_creazione, id_azienda) DO UPDATE 
        SET piena_visibilita = EXCLUDED.piena_visibilita
        RETURNING id
    )
    DELETE FROM scripta.persone_vedenti 
    WHERE id_azienda = %(id_azienda)s
    AND data_creazione = ( SELECT data_creazione FROM data_creazione )
    AND id_doc_detail = %(id_doc)s
    AND id NOT IN (SELECT id FROM id_da_tenere)
    AND id NOT IN (SELECT id FROM altri_id_da_tenere)
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
insert_registri_docproposte = """
    INSERT INTO scripta.registri_docs (
     id_registro, id_doc, numero, anno, id_persona_registrante, id_struttura_registrante, data_registrazione
    )
    SELECT r.id , %(id_doc)s , %(numero_proposta)s, %(anno_proposta)s, %(id_persona_registrazione)s, %(id_struttura_registrazione)s, %(data_creazione)s
    FROM scripta.registri r
    WHERE r.id_azienda = %(id_azienda)s 
    AND r.attivo = true
	and ( (%(tipologia)s in ('PROTOCOLLO_IN_ENTRATA', 'PROTOCOLLO_IN_USCITA')  AND  r.codice = 'PROP_PG')
			OR (%(tipologia)s = 'DETERMINA' AND r.codice ='PROP_DETE')
			OR (%(tipologia)s = 'DELIBERA' AND r.codice ='PROP_DELI' ))
	on conflict (id_registro, id_doc)
	do update 
	set numero = EXCLUDED.numero , anno = EXCLUDED.anno
"""
insert_registri_doc_registrati = """INSERT INTO scripta.registri_docs (
     id_registro, id_doc, numero, anno, id_persona_registrante, id_struttura_registrante, data_registrazione
    )
SELECT r.id , %(id_doc)s , %(numero_registrazione)s, %(anno_registrazione)s,  %(id_persona_registrazione)s, %(id_struttura_registrazione)s, %(data_registrazione)s
    FROM scripta.registri r
    WHERE r.id_azienda = %(id_azienda)s 
    AND r.attivo = true
    and %(numero_registrazione)s is not null 
	and (
  (%(tipologia)s = 'PROTOCOLLO_IN_ENTRATA' AND r.codice = 'PG') OR
  (%(tipologia)s = 'PROTOCOLLO_IN_USCITA' AND r.codice = 'PG') OR
  (%(tipologia)s = 'DETERMINA' AND r.codice = 'DETE') OR
  (%(tipologia)s = 'DELIBERA' AND r.codice = 'DELI') OR
  (%(tipologia)s = 'RGPICO' AND r.codice = 'RGPICO') OR
  (%(tipologia)s = 'RGDETE' AND r.codice = 'RGDETE') OR
  (%(tipologia)s = 'RGDELI' AND r.codice = 'RGDELI')
)	on conflict (id_registro, id_doc)
do update 
	set numero = EXCLUDED.numero , anno = EXCLUDED.anno """
insert_allegati_doc = """
    INSERT INTO scripta.allegati (
        nome, tipo, principale, firmato, 
        ordinale, id_doc, id_allegato_padre, data_inserimento, 
        dettagli, id_esterno, sottotipo, additional_data, version
    ) VALUES ( 
        %(nome)s,
        %(tipo)s,
        CASE
            when %(principale)s != 0 then true::boolean
            else false::boolean
        END,
        %(firmato)s,
        %(ordinale)s,
        %(id_doc)s,
        (   SELECT a.id 
            FROM scripta.allegati a 
            WHERE a.id_esterno = %(id_allegato_padre)s
            AND a.id_doc = %(id_doc)s
        ),
        %(data_inserimento)s,
        %(dettagli)s,
        %(id_esterno)s,
        %(sottotipo)s,
        %(additional_data)s,
        now()
        )
    ON CONFLICT (id_doc, id_esterno, tipo) DO UPDATE
    SET nome = excluded.nome,
        tipo = excluded.tipo,
        principale = excluded.principale,
        firmato = excluded.firmato,
        ordinale = excluded.ordinale,
        id_allegato_padre = excluded.id_allegato_padre,
        data_inserimento = excluded.data_inserimento,
        dettagli = excluded.dettagli,
        sottotipo = excluded.sottotipo,
        additional_data = excluded.additional_data,
        version = excluded.version
    RETURNING id
"""
delete_allegati = """
    DELETE FROM scripta.allegati aa
    WHERE aa.id_doc = %(id_doc)s
    AND not aa.id = ANY(%(id_allegati_da_tenere)s) 
"""
delete_allegati_tutti = """
    DELETE FROM scripta.allegati aa
    WHERE aa.id_doc = %(id_doc)s
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
             ordinale, vedente, sulla_scrivania
        ) 
        SELECT DISTINCT %(id_doc)s, id_persona::integer, id_struttura::integer, ruolo::scripta.ruolo_attore_doc, 
             MIN(ordinale::integer), bool_or(vedente), bool_or(sulla_scrivania)
        FROM (
        VALUES  
            {values}
        ) AS t (id_persona, id_struttura, ruolo, ordinale, vedente , sulla_scrivania)
        GROUP BY 
            id_persona, id_struttura, ruolo 
        ON CONFLICT (id_doc, id_persona, id_struttura, ruolo) DO UPDATE 
        SET sulla_scrivania = EXCLUDED.sulla_scrivania,
            ordinale = EXCLUDED.ordinale,
            version = EXCLUDED.version,
            vedente = EXCLUDED.vedente
        RETURNING id
    )
    DELETE FROM scripta.attori_docs 
    WHERE id_doc = %(id_doc)s
    AND id NOT IN (SELECT id FROM id_da_tenere)
"""

upsert_related_and_delete_the_others="""
    WITH id_da_tenere AS (
        INSERT INTO scripta.related (
            id_doc,  id_persona_inserente, tipo, 
            origine,  descrizione,data_inserimento
        ) 
        SELECT DISTINCT ON (descrizione, tipo) %(id_doc)s,  id_persona_inserente::integer, tipo::scripta.tipo_related, 
            origine::scripta.origine_related,  descrizione::text, TO_TIMESTAMP(REPLACE(data_inserimento::text, 'T', ' '),'YYYY-MM-DD HH24:MI:SS')::timestamptz
        FROM (
        VALUES  
            {values}
        ) AS t ( id_persona_inserente, tipo, origine,  descrizione , data_inserimento)
        GROUP BY 
            id_persona_inserente, descrizione, tipo , origine,  data_inserimento
        ON CONFLICT (id_doc, descrizione, tipo ) DO UPDATE 
        SET 
            id_persona_inserente = EXCLUDED.id_persona_inserente,
            origine = EXCLUDED.origine,
            data_inserimento = EXCLUDED.data_inserimento,
            descrizione = EXCLUDED.descrizione,
            version = EXCLUDED.version
        RETURNING id
    )
    DELETE FROM scripta.related 
    WHERE id_doc = %(id_doc)s
    AND id NOT IN (SELECT id FROM id_da_tenere)
"""
upsert_spedizione="""
    INSERT INTO scripta.spedizioni (
        id_related, id_message, id_mezzo, indirizzo, id_smistamento, annullata, data_inserimento
    )
    SELECT r.id , %(id_message)s, %(id_mezzo)s, jsonb_build_object('cap', null, 'via', null, 'civico', null, 'comune',null, 'nazione', null, 'provincia', null, 'indirizzo', r.descrizione)::jsonb, null, FALSE, r.data_inserimento
    FROM scripta.related r
    WHERE r.tipo = 'MITTENTE'::scripta.tipo_related
    AND r.id_doc = %(id_doc)s
    ON CONFLICT (id_related, id_message ) DO UPDATE 
    SET id_mezzo = EXCLUDED.id_mezzo,
        indirizzo = EXCLUDED.indirizzo,
        id_smistamento = EXCLUDED.id_smistamento,
        annullata = EXCLUDED.annullata,
        data_inserimento = EXCLUDED.data_inserimento,
        version = EXCLUDED.version
"""
delete_spedizione="""
    DELETE FROM scripta.spedizioni
    WHERE id_related in (SELECT r.id FROM scripta.related r WHERE r.tipo = 'MITTENTE'::scripta.tipo_related  AND r.id_doc = %(id_doc)s)
"""
seleziona_id_mezzo="""
    SELECT id FROM scripta.mezzi WHERE descrizione = %(mezzo)s
"""
delete_attori = """
    DELETE FROM scripta.attori_docs 
    WHERE id_doc = %(id_doc)s
"""
get_id_pec = """
    SELECT id FROM baborg.pec 
    WHERE lower(indirizzo) = lower(%(id_pec_mittente)s)
"""
delete_messages_docs = """
    DELETE FROM scripta.messages_docs
    WHERE id_doc = %(id_doc)s
    AND id_message = %(id_message)s
    AND scope = 'PROTOCOLLAZIONE'::scripta.message_doc_scope
"""
insert_messages_docs_pe = """
    INSERT INTO scripta.messages_docs (
        id_doc, id_message, "tipo", "scope"
    ) 
    SELECT %(id_doc)s, %(id_message)s, 'IN'::scripta.tipi_messages_docs, 'PROTOCOLLAZIONE'::scripta.message_doc_scope
    FROM shpeck.messages m
    WHERE m.id = %(id_message)s
    ON CONFLICT (id_doc, id_message, "scope") DO NOTHING
"""
insert_messages_docs_pu = """
    INSERT INTO scripta.messages_docs (
        id_doc, id_message, "tipo", "scope"
    ) 
    SELECT %(id_doc)s, m.id , 'IN'::scripta.tipi_messages_docs, 'PROTOCOLLAZIONE'::scripta.message_doc_scope
    FROM shpeck.messages m
    WHERE m.id_outbox =  substring(%(id_message)s FROM '[0-9]+')::int
    ON CONFLICT (id_doc, id_message, "scope") DO NOTHING
"""
insert_docs_collegi_sindacali_and_delete_the_others = """
    WITH id_da_tenere AS (
        INSERT INTO scripta.collegi_sindacali_docs 
        (id_collegio_sindacale, id_doc) 
        SELECT DISTINCT id_collegio_sindacale, %(id_doc)s 
        FROM (
            VALUES 
                {values}
            ) AS t (id_collegio_sindacale)
        ON CONFLICT DO NOTHING
        RETURNING id
    )
    DELETE FROM scripta.collegi_sindacali_docs 
    WHERE id_doc = %(id_doc)s 
    AND id NOT IN (SELECT id FROM id_da_tenere)  
"""
delete_collegi_sindacali = """
    DELETE FROM scripta.collegi_sindacali_docs 
    WHERE id_doc = %(id_doc)s
"""
delete_related = """
    DELETE FROM scripta.related
    WHERE id_doc = %(id_doc)s
"""
get_collegi_sindacali = """
    SELECT jsonb_object_agg(email || '__' || id_azienda, jsonb_build_object('email', email, 'attivo', attivo, 'predefinita', predefinita, 'id_azienda', id_azienda, 'id', id)) AS collegi_sindacali_map
    FROM scripta.collegi_sindacali
"""
aggiorna_id_strutture_segreteria_su_docs_details = """
    SELECT * FROM scripta.aggiorna_id_strutture_segreteria_su_docs_details(%(id_doc)s)
"""