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
                WHEN  %(riservato)s = true
                    THEN 'RISERVATO'::scripta.visibilita_doc
                WHEN  %(visibilita_limitata)s = true
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
                WHEN  %(riservato)s = true
                    THEN 'RISERVATO'::scripta.visibilita_doc
                WHEN  %(visibilita_limitata)s = true
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
            visibilita = excluded.visibilita,
            id_pec_mittente = excluded.id_pec_mittente,
            version = excluded.version,
            additional_data = excluded.additional_data,
            stato = excluded.stato,
            id_struttura_registrante = excluded.id_struttura_registrante,
            data_registrazione = excluded.data_registrazione,
            data_creazione = excluded.data_creazione
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
            vedente = EXCLUDED.vedente
        RETURNING id
    )
    DELETE FROM scripta.attori_docs 
    WHERE id_doc = %(id_doc)s
    AND id NOT IN (SELECT id FROM id_da_tenere)
"""
delete_firmatari = """
    delete from scripta.firmatari
    where id_doc = %(id_doc)s
"""
insert_firmatari = """
    with id_attori AS (
        SELECT id , id_persona
        FROM scripta.attori_docs 
        WHERE id_doc = %(id_doc)s and ruolo in ('FIRMA', 'DIRETTORE_GENERALE','DIRETTORE_SANITARIO','DIRETTORE_SCIENTIFICO','DIRETTORE_AMMINISTRATIVO')
    )
    INSERT INTO scripta.firmatari (id, id_persona, id_doc, stato, documento_visto, tipologia_firma, ts_firma)
    SELECT DISTINCT id_attori.id, t.id_persona, %(id_doc)s, t.stato::scripta.stati_firmatario, false, t.tipologia_firma::scripta.tipologie_firma, t.ts_firma::timestamptz
    FROM (
        VALUES 
            {values}
        ) AS t (id_persona, tipologia_firma, ts_firma, stato)
    JOIN id_attori on id_attori.id_persona = t.id_persona
    ON CONFLICT DO NOTHING
"""
delete_firmatari_allegati = """
    delete from scripta.firmatari_allegati
    where id_allegato in (
        select id from scripta.allegati where id_doc = %(id_doc)s
    )
"""
insert_firmatari_allegati = """
    with id_allegati AS (
        SELECT id , id_esterno
        FROM scripta.allegati 
        WHERE id_doc = %(id_doc)s 
    ),
    id_attori AS (
        SELECT id , id_persona
        FROM scripta.attori_docs 
        WHERE id_doc = %(id_doc)s and ruolo = 'FIRMA'::scripta.ruolo_attore_doc
    ) 
    INSERT INTO scripta.firmatari_allegati ( id_allegato, id_attore, id_persona_inserente, firmato, tipologia_firma, ts_firma, data_inserimento, tipo_dettaglio_firmato)
    SELECT DISTINCT id_allegati.id, id_attori.id, 1 , t.firmato, t.tipologia_firma::scripta.tipologie_firma, t.ts_firma::timestamptz, now(), t.dettaglio_firmato::scripta.tipi_dettagli_allegati
    FROM (
        VALUES  
            {values}
        ) AS t (id_allegato, id_persona_attore, tipologia_firma, firmato, ts_firma, dettaglio_firmato)
    JOIN id_allegati on id_allegati.id_esterno = t.id_allegato
    JOIN id_attori on id_attori.id_persona = t.id_persona_attore
    ON CONFLICT DO NOTHING
"""

upsert_related_and_delete_the_others="""
    WITH id_da_tenere AS (
        INSERT INTO scripta.related (
            id_doc,  id_persona_inserente, tipo, 
            origine,  descrizione,data_inserimento, id_esterno
        ) 
        SELECT DISTINCT ON (descrizione, tipo) %(id_doc)s,  id_persona_inserente::integer, tipo::scripta.tipo_related, 
            origine::scripta.origine_related,  descrizione::text, TO_TIMESTAMP(REPLACE(data_inserimento::text, 'T', ' '),'YYYY-MM-DD HH24:MI:SS')::timestamptz, id_esterno::text
        FROM (
        VALUES  
            {values}
        ) AS t ( id_persona_inserente, tipo, origine,  descrizione , data_inserimento, id_esterno)
        GROUP BY 
            id_persona_inserente, descrizione, tipo , origine,  data_inserimento, id_esterno
        ON CONFLICT (id_doc, descrizione, tipo, id_esterno ) DO UPDATE 
        SET 
            id_persona_inserente = EXCLUDED.id_persona_inserente,
            origine = EXCLUDED.origine,
            data_inserimento = EXCLUDED.data_inserimento,
            descrizione = EXCLUDED.descrizione,
            id_esterno = EXCLUDED.id_esterno,
            version = EXCLUDED.version
        RETURNING id
    )
    DELETE FROM scripta.related 
    WHERE id_doc = %(id_doc)s
    AND id NOT IN (SELECT id FROM id_da_tenere)
"""
upsert_spedizione="""
    WITH id_da_tenere AS (
        INSERT INTO scripta.spedizioni (
            id_related, id_message, id_mezzo, indirizzo, id_smistamento, annullata, data_inserimento
        )
        SELECT r.id , (SELECT id from shpeck.messages WHERE id = %(id_message)s limit 1) , %(id_mezzo)s, jsonb_build_object('cap', null, 'via', null, 'civico', null, 'comune',null, 'nazione', null, 'provincia', null, 'completo', %(indirizzo)s)::jsonb, null, FALSE, r.data_inserimento
        FROM scripta.related r
        JOIN scripta.docs d on r.id_doc = d.id
        WHERE r.id_esterno = %(id_esterno)s
        AND d.id_esterno = %(guid_doc)s
        ON CONFLICT (id_related, id_message ) DO UPDATE 
        SET id_mezzo = EXCLUDED.id_mezzo,
            indirizzo = EXCLUDED.indirizzo,
            id_smistamento = EXCLUDED.id_smistamento,
            annullata = EXCLUDED.annullata,
            data_inserimento = EXCLUDED.data_inserimento,
            version = EXCLUDED.version
        RETURNING id, id_related
    )
    DELETE FROM scripta.spedizioni 
    WHERE id_related = (SELECT id_related FROM id_da_tenere)
    AND id != (SELECT id FROM id_da_tenere)
"""
delete_spedizione="""
    DELETE FROM scripta.spedizioni
    WHERE id_related in (
        SELECT r.id 
        FROM scripta.related r 
        JOIN scripta.docs d ON d.id = r.id_doc
        WHERE r.tipo = 'MITTENTE'::scripta.tipo_related  
        AND d.id_esterno = %(guid_doc)s
        )
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
select_id_doc_from_id_esterno = """
    SELECT id FROM scripta.docs WHERE id_esterno = %(guid_doc)s and id_azienda = %(id_azienda)s
"""
insert_job_calcola_persone_vedenti = """
    INSERT INTO masterjobs.jobs_notified (
        job_name, job_data, "deferred", object_id,
        object_type, app, wait_object, priority,
        insert_ts, skip_if_already_present
    ) VALUES (
        'CalcolaPersoneVedentiDocJobWorker', json_build_object(
            '@class', 'it.bologna.ausl.internauta.utils.masterjobs.workers.jobs.calcolapersonevedentidoc.CalcolaPersoneVedentiDocJobWorkerData',
            'idDoc', %(id_doc)s
        ), false, %(id_doc)s, 
        'scripta.docs', 'scripta', TRUE, 'NORMAL', 
        now(), FALSE
    )
"""
insert_job_upsert_doc_detail = """
    SELECT scripta.insert_update_doc_detail_job(%(id_doc)s)
"""
delete_from_docs_annullati = """
    DELETE FROM scripta.docs_annullati WHERE id_doc = %(id_doc)s and tipo IN ('ANNULLATO', 'PRE_ANNULLATO')
"""
delete_nota_doc_annullamento = """
    DELETE FROM scripta.note_doc WHERE id_doc = %(id_doc)s and tipo = 'ANNULLAMENTO'
"""
insert_info_annullamento = """
    with insert_nota_doc AS (
        INSERT INTO scripta.note_doc (id_doc, testo, tipo, data_inserimento_riga, id_persona_inserente)
        SELECT id_doc, testo, tipo::scripta.tipo_nota_doc, data_inserimento_riga::timestamptz, id_persona_inserente::integer
        FROM (
            VALUES (%(id_doc)s, %(motivazione)s, 'ANNULLAMENTO', %(data_annullamento)s, %(id_persona_annullante)s)
        ) nota (id_doc, testo, tipo, data_inserimento_riga, id_persona_inserente)
        WHERE %(motivazione)s is not null
        returning id
    )
    INSERT INTO scripta.docs_annullati (
        tipo,"data",id_doc,id_persona_annullante,
        id_nota, id_doc_annullamento, id_struttura_annullante
    ) VALUES (
        %(tipo_annullamento)s::scripta."tipo_annullamento", %(data_annullamento)s::timestamptz, %(id_doc)s, %(id_persona_annullante)s, 
        (select id from insert_nota_doc), (select id from scripta.docs d where d.id_esterno = %(id_esterno_documento_annullamento)s), %(id_struttura_annullante)s
    )
"""
