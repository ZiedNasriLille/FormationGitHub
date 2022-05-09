

/* fonction jour ouvré*/

CREATE TEMP FUNCTION BusinessDateDiff(start_date DATE, end_date DATE, array_jours_feries ANY TYPE) AS (
  (SELECT COUNTIF(MOD(EXTRACT(DAYOFWEEK FROM date), 7) > 1 and jourferie is null)
    FROM 
        UNNEST(GENERATE_DATE_ARRAY(start_date, DATE_SUB(end_date, INTERVAL 1 DAY))) AS date
    left join 
        unnest(array_jours_feries) as jourferie on jourferie = date)
);

INSERT INTO `ddp-dtm-supply-prd-frlm.transport.tf_cmdes_web_client_respect_promesse_V2` 
(

with jours_feries as (
    select 
        ARRAY_AGG(date)  AS array_jours_feries
    from `lmfr-ddp-ods-dev.cdg_supply.jours_feries`
),

 PREMIERE_RESERVATION AS (
    SELECT  distinct num_bu,
            num_mag,
            num_cc,
            num_lig_cc,
            date_creation,
            reflm,
            a_createddate date_rattachement_OA,
            numero_reservation
 FROM
(SELECT  SUBSTR(CO.tc_purchase_orders_id, 1, 3) num_bu,
        CAST(SUBSTR(CO.tc_purchase_orders_id, 5, 3) AS INT64) num_mag,
        SAFE_CAST(SUBSTR(CO.tc_purchase_orders_id, 9, LENGTH(CO.tc_purchase_orders_id)-8) AS INT64) num_cc,
        CAST(CAST(COLI.tc_po_line_id as NUMERIC) AS INT64) num_lig_cc,
        COLI.created_dttm date_creation,
        CAST(COLI.sku AS INT64) reflm,
        AOIA.a_createddate,
        AOIA.a_invobjectid numero_reservation,
        RANK() OVER (PARTITION BY CO.tc_purchase_orders_id,COLI.tc_po_line_id,COLI.created_dttm ORDER BY a_createddate ) ORDRE
FROM    `dfdp-oms.DTM_Oms_LmFr.OMS_A_OrderInventoryAllocation_Photo` AOIA
INNER JOIN `dfdp-oms.DTM_Oms_LmFr.OMS_Customer_Order_Line_Item_Photo` COLI
            ON (cast(aoia.a_orderid as INT64) = coli.purchase_orders_id
            AND cast(aoia.a_orderlineid as INT64) = coli.purchase_orders_line_item_id
            and DATE(COLI.partition_date)>='2020-01-01' /*réduction au 01/01/2019*/)
INNER JOIN `dfdp-oms.DTM_Oms_LmFr.OMS_Customer_Order_Photo` CO
            ON (COLI.purchase_orders_id = CO.purchase_orders_id
            and DATE(CO.partition_date)>='2020-01-01' /*réduction au 01/01/2019*/)
            WHERE 1 = 1
            AND DATE(AOIA.partition_date)>='2020-01-01'/*réduction au 01/01/2019*/
            AND AOIA.a_invobjectid is not null
            AND SUBSTR(CO.tc_purchase_orders_id, 9, LENGTH(CO.tc_purchase_orders_id)-8) not like 'SIMU%'
            AND SUBSTR(CO.tc_purchase_orders_id, 9, LENGTH(CO.tc_purchase_orders_id)-8) not like 'LS%'
            AND SUBSTR(CO.tc_purchase_orders_id, 5, 3) NOT LIKE 'OC-'
            AND COLI.tc_po_line_id not like '%:%'
            AND CO.num_bu = '001'
            and SUBSTR(CO.tc_purchase_orders_id, 5, 3) = '380'
)
WHERE ORDRE = 1) ,
CADENCE as (SELECT * ,
    case when Heure_Liv__Lundi <> 0 then 1 else 0 end cadence_Lundi ,
    case when Heure_Liv__Mardi___ <> 0 then 1 else 0 end cadence_Mardi ,
    case when Heure_Liv__Mercredi <> 0 then 1 else 0 end cadence_Mercredi ,
    case when Heure_Liv__Jeudi <> 0 then 1 else 0 end cadence_Jeudi ,
    case when Heure_Liv__Vendredi <> 0 then 1 else 0 end cadence_Vendredi
     
    FROM `lmfr-ddp-ods-dev.supply_sandbox.Cadence` 
    where Type_Flux='CC'
),

cause_dispo_retard_OA as (
    select 	distinct wlf.num_cmd, 
			wlf.date_cre_cmd ,
			--wlf.numero_contenant, 
			delai_OA_CC,
			wlf.num_ett , 
			wlf.num_art,
            wlf.id_unique_contenant, 
			Coalesce(wlf.cause_dispo_retard_OA, 0) cause_dispo_retard_OA  
	from (	select  CASE WHEN Nb_cadence_semaine = 1 and delai_OA_CC > 5 then 1 /*cause 2 -1 */
					when Nb_cadence_semaine > 1 and delai_OA_CC > 3 then 2   /*cause 2 -2 */
					when Nb_cadence_semaine is null and delai_OA_CC > 3 then 3   /*cause 2 - 3  */ else 0 end cause_dispo_retard_OA, 
                    num_cmd,
                    date_cre_cmd,
                    num_ett,
                    num_art,
                    id_unique_contenant,
                    delai_OA_CC
			from 	(select cadence_Lundi + cadence_Mardi + cadence_Mercredi + cadence_Jeudi + cadence_Vendredi as Nb_cadence_semaine, 
                            case when extract(HOUR FROM PREMIERE_RESERVATION.date_rattachement_OA )  = 6
                                then  BusinessDateDiff(WL.date_cre_cmd,date(PREMIERE_RESERVATION.date_rattachement_OA), jours_feries.array_jours_feries) -1
                                else  BusinessDateDiff(WL.date_cre_cmd,IFNULL(date(PREMIERE_RESERVATION.date_rattachement_OA),CURRENT_DATE), jours_feries.array_jours_feries) 
                            end as delai_OA_CC,
                            WL.num_cmd,
                            WL.date_cre_cmd ,
                            WL.num_ett , 
			                WL.num_art,
                            WL.id_unique_contenant
					from 	`ddp-dtm-supply-prd-frlm.transport.tf_cmdes_web_client_respect_promesse_V2_staging` WL
					LEFT OUTER JOIN PREMIERE_RESERVATION ON (WL.num_ett = LPAD(cast(PREMIERE_RESERVATION.num_mag as string),3,'0') AND WL.num_cmd = PREMIERE_RESERVATION.num_cc AND WL.customer_order_line_number = PREMIERE_RESERVATION.num_lig_cc AND WL.num_art = PREMIERE_RESERVATION.reflm)
					left join CADENCE CAD on CAD.MAGASIN =cast(WL.NUM_ETT as int64) and CAD.code_fournisseur =WL.NUM_FOUCOM and WL.retard_non_annule =1
                    left join jours_feries on 1=1
                    )
		) WLF

),

cause_dmg_coll_rupt_asso as (
     select distinct coalesce(DC.Dommage_Collateral_rupt_asso,0) cause_dmg_coll_rupt_asso ,wl.num_cmd, wl.date_cre_cmd ,
 wl.num_ett , wl.type_reservation, wl.num_art, wl.id_unique_contenant from `ddp-dtm-supply-prd-frlm.transport.tf_cmdes_web_client_respect_promesse_V2_staging` as WL
 left join 
        (select distinct wl2.num_cmd, wl2.date_cre_cmd , wl2.num_ett , wl2.type_reservation, wl2.num_art , wl2.numero_contenant, wl2.id_unique_contenant , 1 as Dommage_Collateral_rupt_asso 
            from `ddp-dtm-supply-prd-frlm.transport.tf_cmdes_web_client_respect_promesse_V2_staging` as WL2 
            inner join (select SWL.*, cause_dispo_retard_OA from`ddp-dtm-supply-prd-frlm.transport.tf_cmdes_web_client_respect_promesse_V2_staging`SWL 
                                    left join cause_dispo_retard_OA DROA
                                    on SWL.num_art = DROA.num_art and SWL.num_cmd=DROA.num_cmd and SWL.date_cre_cmd= DROA.date_cre_cmd AND SWL.num_ett = DROA.num_ett ) as WL

            on wl.num_cmd = wl2.num_cmd and wl.date_cre_cmd=wl2.date_cre_cmd and wl.num_ett=wl2.num_ett 
            and wl.code_entrepot = wl2.code_entrepot
            and  wl.type_reservation<>wl2.type_reservation
            and (wl.customer_order_line_number <>wl2.customer_order_line_number)
            and (wl.date_generation_DO=WL2.date_generation_DO or (wl.date_generation_DO is null and WL2.date_generation_DO is null))
            -- équivalent à : cause_retard_generation_do = 1 
            /*and (date(WL2.datetime_generation_DO) > WL2.dat_liv_initiale) or
            (format_timestamp('%H:%M:%S',wl2.datetime_generation_do) <  '06:30:00' and  WL2.delai_livraison_post_do < WL2.delai_contractuel_expedition) or     
            (format_timestamp('%H:%M:%S',wl2.datetime_generation_do) >= '06:30:00' and  WL2.delai_livraison_post_do < WL2.delai_contractuel_expedition - 1) */
            and wl2.cause_retard_generation_do = 1 
            and ( (wl.type_reservation='PO (stock futur)'  and  WL.categorie_entrepot = 'Autres') or 
            (WL.type_reservation='PO (stock futur)' AND cause_dispo_retard_OA >=1) or 
            (WL.type_reservation='PO (stock futur)' AND IFNULL(DATE(wl.date_livraison_entrepot_prevu),'2000-01-01') >= wl.date_liv_initiale ) or 
            (WL.type_reservation='PO (stock futur)' AND IFNULL(DATE(wl.date_reception_entrepot),'2099-01-01') > IFNULL(DATE(wl.date_livraison_entrepot_prevu),'2000-01-01')) or 
            (WL.type_reservation='PO (stock futur)' AND wl.COD_BLOCAGE is not null AND DATE_ADD(IFNULL(DATE(wl.date_reception_entrepot),'2099-01-01'), INTERVAL IFNULL(wl.NB_JOUR_BLOCAGE,0) DAY) > IFNULL(DATE(wl.date_livraison_entrepot_prevu),'2000-01-01') ) or 
            (WL.type_reservation='PO (stock futur)' AND wl.cause_retard_generation_do = 1)) 
            and   ( wl.type_reservation='PO (stock futur)' and wl.retard_non_annule =1) 
            )  AS DC
on DC.num_cmd= WL.num_cmd and DC.date_cre_cmd = WL.date_cre_cmd  and DC.num_ett=WL.num_ett and  DC.num_art = WL.num_art and  (DC.numero_contenant = WL.numero_contenant or (DC.numero_contenant is null and WL.numero_contenant is null))),

cause_dmg_coll_IT as (
     select distinct coalesce(DC.IT_Dommage_Collateral,0) IT_Dommage_Collateral ,wl.num_cmd, wl.date_cre_cmd ,
 wl.num_ett , wl.type_reservation, wl.num_art, wl.numero_contenant, wl.id_unique_contenant from `ddp-dtm-supply-prd-frlm.transport.tf_cmdes_web_client_respect_promesse_V2_staging` as WL
 left join 
        (select distinct wl2.num_cmd, wl2.date_cre_cmd , wl2.num_ett , wl2.type_reservation, wl2.num_art , wl2.numero_contenant, wl2.id_unique_contenant , 1 as IT_Dommage_Collateral 
            from `ddp-dtm-supply-prd-frlm.transport.tf_cmdes_web_client_respect_promesse_V2_staging`as WL2 
            inner join `ddp-dtm-supply-prd-frlm.transport.tf_cmdes_web_client_respect_promesse_V2_staging` as WL
            on wl.num_cmd=wl2.num_cmd and wl.date_cre_cmd=wl2.date_cre_cmd and wl.num_ett=wl2.num_ett 
            and wl.code_entrepot = wl2.code_entrepot
            and  wl.type_reservation<>wl2.type_reservation
            and (wl.customer_order_line_number <>wl2.customer_order_line_number)
            and wl2.cause_retard_generation_do = 1 
            and (wl.date_generation_DO=WL2.date_generation_DO or (wl.date_generation_DO is null and WL2.date_generation_DO is null))
			AND ((wl.Cause_IT_Date_OMS = 1) or
            (wl.Cause_IT_SOURCED = 1) or 
            /*(DATETIME_DIFF(IFNULL(WL.datetime_creation_allocation,CURRENT_DATETIME ), DATETIME(WL.date_cre_cmd,PARSE_TIME("%H:%M:%S", WL.heure_cre_cmd)), MINUTE) > 60) or */
            (WL.top_back_ordered = 1) or
            /*(wl.cause_it_transport= 1) or
            (wl.top_transport_absence_donnee_expedito = 1) or */
            (wl.Cause_appro_produit_sur_stock = 1 ) or
            (DATETIME_DIFF(DATETIME(wl.date_cre_cmd,PARSE_TIME("%H:%M:%S", wl.heure_cre_cmd)),IFNULL(wl.datetime_creation_allocation,CURRENT_DATETIME ),HOUR) < 0 and wl.retard_non_annule =1
			    AND DATETIME_DIFF(DATETIME(wl2.date_cre_cmd,PARSE_TIME("%H:%M:%S", wl2.heure_cre_cmd)),IFNULL(wl2.datetime_creation_allocation,CURRENT_DATETIME ),HOUR) = 0 and wl.retard_non_annule =1))
 --           and ( wl.allocation_status_OMS ='200 - Sourced' and wl.retard_non_annule =1) 
 --           and (wl2.allocation_status_OMS ='400 - Allocated' and wl2.retard_non_annule =1)
            and   ( wl.type_reservation='PO (stock futur)' and wl.retard_non_annule =1) 
            )  AS DC
on DC.num_cmd= WL.num_cmd and DC.date_cre_cmd = WL.date_cre_cmd  and DC.num_ett=WL.num_ett and  DC.num_art = WL.num_art 
and  (DC.numero_contenant = WL.numero_contenant or (DC.numero_contenant is null and WL.numero_contenant is null))),


TABLE_FINALE as (
SELECT distinct wl.*,
 nb_ligne_commande as nb_ligne_dans_cmd, 
 IT_Dommage_Collateral, 
 cause_dmg_coll_rupt_asso, 
 delai_OA_CC, cause_dispo_retard_OA, 

CASE  
  when top_annulation = 1 then "Annulation Ligne"
  when TOP_RETARD_EXPEDITO = 0 then "OK - Ligne à l'heure" 
  WHEN DATETIME_DIFF(IFNULL(WL.datetime_creation_allocation,CURRENT_DATETIME ), datetime(date_valid_cmd), MINUTE) > 60 THEN "VENTE - Blocage payement commande"
  WHEN Cause_IT_Date_OMS = 1  and Canal_origine= 'MAG' and COD_ATPREP is not null then "IT -Appel ATP KO"
    WHEN Cause_IT_Date_OMS = 1  and Canal_origine= 'MAG' and COD_ATPREP is null then "VENTE - Forçage date livraison"
    WHEN Cause_IT_Date_OMS = 1   then "IT - Calcul de la promesse"
  --when Cause_IT_SOURCED = 1 then "IT - Lignes au statut Sourced"
 -- WHEN IT_Dommage_Collateral_Rupture = 1 then  "IT - Dommage collateral Rupture"
  WHEN WL.top_back_ordered = 1  THEN "IT - Rupture entrepôt écart stock WMS OMS"
  when IT_Dommage_Collateral=1 then "IT - Dommage collateral"
  --APPRO
  WHEN WL.type_reservation='PO (stock futur)' AND WL.categorie_entrepot = 'Autres' THEN 'DISPO - Cause Weldom'
  when WL.type_reservation='PO (stock futur)' AND cause_dispo_retard_OA >=1  AND IFNULL(DATE(wl.date_reception_entrepot),'2099-01-01') > IFNULL(DATE(wl.date_livraison_entrepot_prevu),'2000-01-01') then 'DISPO - Cause retard OA'
  WHEN WL.type_reservation='PO (stock futur)' AND IFNULL(DATE(wl.date_livraison_entrepot_prevu),'2000-01-01') >= wl.date_liv_initiale THEN 'DISPO - IT date de livraison four > promesse'
  when WL.type_reservation='PO (stock futur)' AND IFNULL(DATE(wl.date_reception_entrepot),'2099-01-01') > IFNULL(DATE(wl.date_livraison_entrepot_prevu),'2000-01-01') then 'DISPO - Retard livraison'
  when WL.type_reservation='PO (stock futur)' AND wl.COD_BLOCAGE is not null AND DATE_ADD(IFNULL(DATE(wl.date_reception_entrepot),'2099-01-01'), INTERVAL IFNULL(NB_JOUR_BLOCAGE,0) DAY) > IFNULL(DATE(wl.date_livraison_entrepot_prevu),'2000-01-01')  and CAUSE_STOCK_BLOQUE="Cause stock bloqué Appro" THEN "DISPO - retard stock bloqué APPRO"
  when WL.type_reservation='PO (stock futur)' AND wl.COD_BLOCAGE is not null AND DATE_ADD(IFNULL(DATE(wl.date_reception_entrepot),'2099-01-01'), INTERVAL IFNULL(NB_JOUR_BLOCAGE,0) DAY) > IFNULL(DATE(wl.date_livraison_entrepot_prevu),'2000-01-01')  and CAUSE_STOCK_BLOQUE="Cause stock bloqué fournisseur" THEN "DISPO - retard stock bloqué FOUR"
  when WL.type_reservation='PO (stock futur)' AND cause_retard_generation_do = 1 then "DISPO Cause retard génération DO"
  when cause_dmg_coll_rupt_asso = 1 then 'DISPO - Cause dommage collateral rupture associée'
  
  --when Cause_appro_produit_sur_stock= 1 then "APPRO- Cause Appro produit sur stock"
  --when top_cause_opelog = 1 then "ENT - Cause OPELOG"
  --Ligne avec DO Annulé 
  when top_DO_annule = 1 then 'ENT - Non servis (DO annulé)'
  when Cause_retard_valorisation = 1 then 'ENT - Retard en entrée'
  when Cause_Retard_en_preparation = 1 then 'ENT - Retard en préparation'
  when ((CAUSE_STOCK_BLOQUE = 'Cause stock bloqué entrepôt' and NB_JOUR_BLOCAGE > 0 and Retard_en_sortie = 1 )
        or( CAUSE_STOCK_BLOQUE is not null and NB_JOUR_BLOCAGE > 1 and Retard_en_sortie = 1 ) )then 'ENT - Retard stock bloqué cause opélog'


  /*
  WHEN DATETIME_DIFF(IFNULL(WL.datetime_creation_allocation,CURRENT_DATETIME ), DATETIME(WL.date_cre_cmd,PARSE_TIME("%H:%M:%S", WL.heure_cre_cmd)), MINUTE) > 60 THEN "IT/VENTE - Allocation non calculée dans l'heure par l'OMS"
  WHEN Cause_IT_Date_OMS = 1 then "IT - Date OMS vs Date GESCO"
  WHEN IT_Dommage_Collateral_Rupture = 1 then  "IT - Dommage collateral Rupture"
  WHEN Case when WL.Cause_appro_produit_sur_stock = 1 and WL.type_reservation='OH (on hand = sur site)' and WL.top_back_ordered = 1 then 1 else 0 end = 1 THEN "IT - Rupture entrepôt écart stock WMS OMS'"
  when IT_Dommage_Collateral=1 then "IT - Dommage collateral"
  when cause_dispo_retard_OA >=1 then 'APPRO - Cause retard OA'
  when cause_dmg_coll_rupt_asso = 1 then 'APPRO - Cause dommage collateral rupture associée'
  when Cause_appro_produit_sur_commande = 1 then "APPRO - Cause Appro produit sur commande"
  when Cause_appro_produit_sur_stock= 1 then "APPRO- Cause Appro produit sur stock"
  when cause_retard_generation_do = 1 then "APPRO Cause retard génération DO"
  when top_cause_opelog = 1 then "ENT - Cause OPELOG"*/
--  when cause_report_client = 1 then "CLI - Report client"
 -- when cause_it_transport= 1 then "IT - Info transport"

 /* -- 20211214 - backlog 56 - Modif règle interval 45 RMONTA11 45*/
when datetime_preparation_started is not null
  and  (DATE_DIFF(datetime_preparation_started,datetime_generation_do,HOUR ) > 24 OR datetime_preparation_started > datetime(WL.date_cutoff, parse_time( '%H:%M:%S', WL.heure_cutoff) ))
  and WL.datetime_expedition > DATETIME_ADD(datetime(WL.date_cutoff, parse_time( '%H:%M:%S', WL.heure_cutoff) ), INTERVAL 0 MINUTE)
  then 'ENT - Retard préparation non démarrée (DO disponible)'



  /*-- 20211214 - backlog 56 - RMONTA11  Suppression de la règle Retard_en_sortie = 1 and + règle interval 45 min  */
  when WL.datetime_expedition > DATETIME_ADD(datetime(WL.date_cutoff, parse_time( '%H:%M:%S', WL.heure_cutoff) ), INTERVAL   0 MINUTE) then "ENT - Retard en sortie (pb FIFO ou manque affrètement)"
  when cause_En_balance_entrepot_ou_transport = 1 and WL.datetime_expedition > DATETIME_ADD(datetime(WL.date_cutoff, parse_time( '%H:%M:%S', WL.heure_cutoff) ), INTERVAL   0 MINUTE) then "ENT - Retard en sortie (pb FIFO ou manque affrètement)"
  when top_transport_absence_donnee_expedito = 1 then "IT - Perte de données expédition"
  
  /*when top_retard_transport_delai_consignation = 1 then "TPT - 1 - Cause consignation et prise en charge informatique"
  when top_retard_transport_delai_prise_en_charge = 1 then "TPT - 2 - Cause Délai prise en charge"
  when cause_transport_retard_prise_de_rdv = 1 then "TPT - 3 - Transport prise de RDV"
  when transport_retard_acheminement = 1 then  "TPT - 4 - Transport retard acheminement"
  when Top_retard_prise_en_charge = 1 then  "TPT - 5 - Transport autre" */
  when top_cause_tpt_desynchro_flux_physique_et_it = 1 then "TPT 1 - Désynchro expé WMS vs envoi EDI Expedito" /*renommage ancien nom : Cause désynchronisation flux Physique et IT" */ 
 -- when top_cause_tpt_consignation_et_prise_en_charge_it = 1 and top_cause_tpt_delai_prise_en_charge = 0 then "TPT 2 - Désynchro envoi EDI expedito Transporteur" /*renommage ancien nom : Cause consignation et prise en charge IT*/
  --MARIE : Nouvelle RG : Découper en 3 sous causes : 1/VIR, WARNING,GEODIS >12h 2/Retard intégration EDI>1h 3/Evt "OK EDI" non intégré
  --MARIE / 20211005/Modification : Suppression du top sur le délai de prise en charge (voir mail de Marie L. 20211005 à 17h44 ) 
  when top_cause_tpt_consignation_et_prise_en_charge_it = 1
   then "TPT 2.1 - VIR, WARNING,EUROMATIC>12h"  
  when top_cause_tpt_consignation_et_prise_en_charge_it = 2 
    then "TPT 2.2 - Retard intégration EDI>1h" 
   when top_cause_tpt_consignation_et_prise_en_charge_it = 3 
   then "TPT 2.3 - Evt OK EDI non intégré" 
  /*when top_cause_tpt_consignation_et_prise_en_charge_it = 1 and top_cause_tpt_delai_prise_en_charge = 0 
   then "TPT 2.1 - VIR, WARNING,GEODIS >12h"  
   when top_cause_tpt_consignation_et_prise_en_charge_it = 2 and top_cause_tpt_delai_prise_en_charge = 0 
    then "TPT 2.2 - Retard intégration EDI>1h" 
   when top_cause_tpt_consignation_et_prise_en_charge_it = 3 and top_cause_tpt_delai_prise_en_charge = 0 
   then "TPT 2.3 - Evt ''OK EDI'' non intégré" */
  
  when top_cause_tpt_probleme_logistique_expedition_manquante_prestataire = 1 then "TPT 3 - Bippé non chargé"
  when top_cause_tpt_delai_prise_en_charge = 1 then "TPT 4 - Cause Délai prise en charge"
   when top_cause_tpt_delai_prise_rendez_vous = 1 then "TPT 5 - Délai Transporteur prise de rendez-vous"
  when top_cause_tpt_client = 1 then "TPT 6 - Choix Client"
  when top_cause_tpt_avarie = 1 then "TPT 7 - Casses / Pertes / ..."
  ELSE 
    case when nom_transporteur_agg = "En attente" and tr_datetime_expedition_wms is null then "ENT - Retard préparation non démarrée (DO disponible)" else "TPT 8 - Retard Acheminement" 
    END
END as responsabilite 
 FROM `ddp-dtm-supply-prd-frlm.transport.tf_cmdes_web_client_respect_promesse_V2_staging` WL
 left join cause_dispo_retard_OA DROA
    on WL.num_art = DROA.num_art and WL.num_cmd=DROA.num_cmd and WL.date_cre_cmd= DROA.date_cre_cmd AND WL.num_ett = DROA.num_ett 
    AND WL.id_unique_contenant = DROA.id_unique_contenant
 left join cause_dmg_coll_rupt_asso DCRA
 on WL.num_art = DCRA.num_art and WL.num_cmd=DCRA.num_cmd and WL.date_cre_cmd= DCRA.date_cre_cmd AND WL.num_ett = DCRA.num_ett AND WL.id_unique_contenant = DCRA.id_unique_contenant
  left join cause_dmg_coll_IT DCI
 on WL.num_art = DCI.num_art and WL.num_cmd=DCI.num_cmd and WL.date_cre_cmd= DCI.date_cre_cmd AND WL.num_ett = DCI.num_ett AND WL.id_unique_contenant = DCI.id_unique_contenant
where  date_liv_initiale >= date_sub(CURRENT_DATE(), INTERVAL {{vars.delta}} MONTH)
 ),

--- RMONTA11 : 2022-01-280
--- Calculs à la commande : 
--- Une commande est en retard si au moins une ligne est en retard
--- Une commmande est non annulée s'il y a 0 ligne annulée, 
--- Une commande est non choix client s'il y a 0 ligne en choix client 
promesse_cmd as 
(
SELECT  
    concat(num_ett,'-',num_cmd,'-',date_cre_cmd)  as Id_unique_commande, 
    num_ett , 
    num_cmd , 
    date_cre_cmd , 
    max(case when responsabilite = "TPT 6 - Choix Client" then 1 else 0 end) as top_choix_client_commande   , 
    max(case when top_retard_expedito = 1 then 1 else 0 end)  as top_retard_commande , 
    max(case when top_annulation = 1 then 1 else 0 end) as top_annulation_commande  
    from TABLE_FINALE
    group by 1 , 2 , 3 , 4 
    having  max(case when responsabilite = "TPT 6 - Choix Client" then 1 else 0 end) = 0 
            and max(case when top_retard_expedito = 1 then 1 else 0 end)  = 1 
            and max(case when top_annulation = 1 then 1 else 0 end)  = 0  
)

 select cd.*,  
case when cd.retard_non_annule >0 and cd.responsabilite <>"TPT 6 - Choix Client" then cd.retard_non_annule else 0 end as RETARD_NON_ANNULE_NON_CHOIX_CLIENT,
case when cd.retard_non_annule_j1 >0 and cd.responsabilite <>"TPT 6 - Choix Client" then cd.retard_non_annule else 0 end as RETARD_NON_ANNULE_NON_CHOIX_CLIENT_J1,
case when cd.retard_non_annule_j2 >0 and cd.responsabilite <>"TPT 6 - Choix Client" then cd.retard_non_annule else 0 end as RETARD_NON_ANNULE_NON_CHOIX_CLIENT_J2,
case when cd.retard_non_annule_j3 >0 and cd.responsabilite <>"TPT 6 - Choix Client" then cd.retard_non_annule else 0 end as RETARD_NON_ANNULE_NON_CHOIX_CLIENT_J3,
case when cd.retard_non_annule_j4 >0 and cd.responsabilite <>"TPT 6 - Choix Client" then cd.retard_non_annule else 0 end as RETARD_NON_ANNULE_NON_CHOIX_CLIENT_J4,
case when cd.retard_non_annule_j5 >0 and cd.responsabilite <>"TPT 6 - Choix Client" then cd.retard_non_annule else 0 end as RETARD_NON_ANNULE_NON_CHOIX_CLIENT_J5,
concat(cd.num_ett,'-',cd.num_cmd,'-',cd.date_cre_cmd) as Id_unique_commande,
pm.Id_unique_commande as Id_unique_commande_non_annule_non_choix_client_en_retard
from TABLE_FINALE cd left join promesse_cmd pm using(num_ett,num_cmd,date_cre_cmd)
 
 
 
)

-- -- select count(distinct Id_unique_commande_non_annule_non_choix_retard), count(distinct Id_unique_ligne_non_annule_non_choix_retard)
-- -- from (
-- SELECT  
-- concat(num_ett,'-',num_cmd,'-',date_cre_cmd)  as Id_unique_commande, 
-- case when RETARD_NON_ANNULE_NON_CHOIX_CLIENT > 0 then  concat(num_ett,'-',num_cmd,'-',date_cre_cmd) else null end as Id_unique_commande_non_annule_non_choix_retard , 
-- case when RETARD_NON_ANNULE_NON_CHOIX_CLIENT > 0 then id_unique_contenant else null end as Id_unique_ligne_non_annule_non_choix_retard , 
-- *
-- FROM `ddp-dtm-supply-prd-frlm.transport.tf_cmdes_web_client_respect_promesse_V2` 
--  WHERE concat(num_ett,'-',num_cmd,'-',date_cre_cmd) in ('380-429172-2021-12-27','380-651272-2022-01-02','380-999503-2021-12-26') 
-- -- ); 
-- ;