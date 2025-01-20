insert into esportazioni.cannoneggiamenti
  (id_oggetto, tipo_oggetto, operazione, priority)
SELECT guid_delibera, 'deli'::esportazioni.tipi_oggetto, 'UPDATE', 5
FROM deli.delibere
ON CONFLICT (id_oggetto, tipo_oggetto, operazione, in_esecuzione) DO nothing;

insert into esportazioni.cannoneggiamenti
  (id_oggetto, tipo_oggetto, operazione, priority)
SELECT guid_determina, 'dete'::esportazioni.tipi_oggetto, 'UPDATE', 5
FROM dete.determine
ON CONFLICT (id_oggetto, tipo_oggetto, operazione, in_esecuzione) DO nothing;

insert into esportazioni.cannoneggiamenti
  (id_oggetto, tipo_oggetto, operazione, priority)
SELECT guid_documento, CASE WHEN movimentazione = 'in' THEN 'pico_pe'::esportazioni.tipi_oggetto ELSE 'pico_pu'::esportazioni.tipi_oggetto END, 'UPDATE', 5
FROM procton.documenti
ON CONFLICT (id_oggetto, tipo_oggetto, operazione, in_esecuzione) DO nothing;

insert into esportazioni.cannoneggiamenti
  (id_oggetto, tipo_oggetto, operazione, priority)
select rg.id, rg.codice_registro::esportazioni."tipi_oggetto" ,'UPDATE', 5 
from bds_tools.registri_giornalieri rg
ON CONFLICT (id_oggetto, tipo_oggetto, operazione, in_esecuzione) DO nothing;

insert into esportazioni.cannoneggiamenti
	(id_oggetto, tipo_oggetto, operazione, priority)
select d.guid_delibera, 'related_deli','UPDATE', 5  
from deli.delibere d
ON CONFLICT (ID_OGGETTO,TIPO_OGGETTO,"operazione",IN_ESECUZIONE) DO NOTHING;
               
insert into esportazioni.cannoneggiamenti
	(id_oggetto, tipo_oggetto, operazione, priority)
select d.guid_determina , 'related_dete','UPDATE', 5  
from dete.determine d
ON CONFLICT (ID_OGGETTO,TIPO_OGGETTO,"operazione",IN_ESECUZIONE) DO NOTHING;
               
 insert into esportazioni.cannoneggiamenti
	(id_oggetto, tipo_oggetto, operazione, priority)
select d.guid_documento, CASE WHEN d.movimentazione = 'in' THEN 'related_pe'::esportazioni.tipi_oggetto ELSE 'related_pu'::esportazioni.tipi_oggetto END,'UPDATE', 5  
from procton.documenti d
ON CONFLICT (ID_OGGETTO,TIPO_OGGETTO,"operazione",IN_ESECUZIONE) DO NOTHING;
