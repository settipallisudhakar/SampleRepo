
SET hive.auto.CONVERT.JOIN=false;
SET hive.auto.CONVERT.JOIN.noconditionaltask=false;
SET hive.auto.CONVERT.sortmerge.JOIN=false;
SET hive.optimize.skewjoin = true;

INSERT overwrite TABLE ekl_logistics_profiler_revenue_new_hive_fact
--changes 3pl handover charges 
--NDD air flag 
--returns processing charges 
--replacement 10 rs discount 
--ROI express and standard 
SELECT   x.vendor_tracking_id, 
         x.shipment_fa_flag, 
         x.seller_id, 
         x.lzn, 
         lookup_date(x.shipment_dispatch_datetime)                        AS shipment_dispatch_date_key, 
         lookup_time(x.shipment_dispatch_datetime)                        AS shipment_dispatch_time_key, 
         lookup_date(x.shipment_first_received_at_datetime)               AS shipment_received_at_date_key,
         lookup_time(x.shipment_first_received_at_datetime)               AS shipment_received_at_time_key,
         lookup_date(x.received_at_origin_facility_datetime)              AS shipment_received_at_origin_facility_date_key,
         lookup_time(x.received_at_origin_facility_datetime)              AS shipment_received_at_origin_facility_time_key,
         x.shipment_value                                                 AS shipment_value, 
         (x.billable_weight/1000)                                         AS billable_weight,
		 IF(x.sla < 4,'<= 3 Days','> 3 Days')                             as sla_bucket,
         concat(x.weight_bucket,'-',(x.weight_bucket+500))                AS weight_bucket, 
         sum(x.delivery_revenue)                                          AS delivery_revenue,
         sum(x.forward_revenue)                                           AS forward_revenue,
         sum(x.rto_revenue)                                               AS rto_revenue, 
         sum(x.cod_revenue)                                               AS cod_revenue, 
         sum(x.priority_shipment_revenue)                                 AS priority_shipment_revenue,
         sum(x.priority_shipment_incremental_revenue)                     AS priority_shipment_incremental_revenue,
         sum(x.priority_shipment_revenue+x.cod_revenue)                   AS vas_revenue, 
         sum(x.rvp_revenue)                                               AS rvp_revenue, 
         sum(x.first_mile_revenue)                                        AS first_mile_revenue,
         sum(x.handover_revenue_3pl)                                      AS handover_revenue_3pl,
         sum(x.pos_revenue)                                               AS pos_revenue, 
         x.asp_bucket                                                     AS asp_bucket, 
         sum(x.risk_surcharge)                                            AS risk_surcharge, 
         sum(x.forward_revenue+x.rto_revenue+x.priority_shipment_revenue) AS rto_revenue_total,
         x.first_mile_revenue_flag                                        AS first_mile_revenue_flag,
         lookupkey('pincode',x.source_pincode)                            AS source_pincode_key,
         lookupkey('pincode',x.destination_pincode)                       AS destination_pincode_key,
         x.air_flag                                                       AS air_flag, 
         x.reverse_shipment_type                                          AS reverse_shipment_type, 
         x.ekart_lzn_flag                                                 AS ekart_lzn_flag, 
         sum(x.return_processing_charges)                                 AS return_processing_charges, 
         sum(x.replacement_discount)                                      AS replacement_discount 
FROM     ( 
                          SELECT           s.vendor_tracking_id, 
                                           s.shipment_first_received_at_datetime, 
                                           rc.lzn, 
                                           s.actual_month, 
                                           s.ekart_lzn_flag, 
                                           s.sla, 
                                           s.payment_mode, 
                                           s.reverse_shipment_type, 
                                           s.shipment_dispatch_datetime, 
                                           s.received_at_origin_facility_datetime, 
                                           s.ekl_shipment_type, 
                                           s.shipment_priority_flag, 
                                           s.payment_type, 
                                           s.shipment_value, 
                                           s.shipment_fa_flag, 
                                           s.seller_id, 
                                           s.logistics_promise_datetime, 
                                           s.ekl_fin_zone, 
                                           s.billable_weight, 
                                           s.weight_bucket, 
                                           s.asp_bucket, 
                                           s.air_flag, 
                                           s.source_pincode, 
                                           s.destination_pincode, 
                                           IF(shipment_carrier = 'FSD', IF(sla >= rc.min_promised_sla AND sla <= rc.max_promised_sla, IF(s.shipment_priority_flag = 'Normal' OR s.shipment_priority_flag IS NULL, IF(s.ekl_shipment_type = 'forward' AND s.shipment_current_status NOT IN ('pickup_leg_complete','pickup_leg_completed'), rc.forward_rate, 0 ), 0 ), 0 ), 0 ) AS delivery_revenue,
                                           IF(s.shipment_carrier = 'FSD' AND s.seller_type IN ('Non-FA', 'FA', 'WSR'), s.shipment_value * 0.001, 0 ) AS risk_surcharge,
                                           IF(shipment_carrier = 'FSD', IF(sla >= rc.min_promised_sla AND sla <= rc.max_promised_sla, IF((s.shipment_priority_flag = 'Normal' OR s.shipment_priority_flag IS NULL), IF(s.ekl_shipment_type NOT IN ('rvp', 'merchant_return') AND s.shipment_current_status NOT IN ('pickup_leg_complete', 'pickup_leg_completed'), rc.forward_rate, 0 ), 0 ), 0 ), 0 ) AS forward_revenue,
                                           IF(shipment_carrier = 'FSD', IF(sla >= rc.min_promised_sla AND sla <= 20000, IF(lower(s.ekl_shipment_type) LIKE '%rto%', rc.rto_rate, 0 ), 0 ), 0 ) AS rto_revenue,
                                           IF(shipment_carrier = 'FSD', IF(sla >= rc.min_promised_sla AND sla <= 20000, IF(lower(s.ekl_shipment_type) LIKE '%rto%' OR lower(ekl_shipment_type) LIKE '%rvp%' , IF(s.seller_type = 'Non-FA', 10, 5 ) , 0 ), 0 ), 0 ) AS return_processing_charges, 
                                           IF(shipment_carrier = 'FSD', IF(((s.payment_type = 'COD' AND ( s.payment_mode = 'COD' OR s.payment_mode IS NULL) AND s.ekl_shipment_type = 'forward' ) AND s.shipment_current_status NOT IN ('pickup_leg_complete', 'pickup_leg_completed')), IF(s.shipment_value<20000, rc.cod_collection_charge_lt_20k, rc.cod_collection_charge_gte_20k ) , 0 ), 0 ) AS cod_revenue,
                                           IF(shipment_carrier = 'FSD', IF(s.payment_type = 'COD' AND s.ekl_shipment_type = 'forward', IF(s.payment_mode = 'POS', 0.012 * s.shipment_value, 0 ), 0 ), 0 ) AS pos_revenue,
                                           IF(shipment_carrier = 'FSD', IF(sla >= rc.min_promised_sla AND sla <= rc.max_promised_sla, (IF(s.shipment_priority_flag LIKE 'NDD%', IF(s.air_flag = 1,rc.ndd_charge_air,rc.ndd_charge) , 0 ) + IF(s.shipment_priority_flag LIKE 'SDD%', rc.sdd_charge, 0 ) ), 0 ), 0 ) AS priority_shipment_revenue,
                                           --priority  incremental revenue 
                                           IF(shipment_carrier = 'FSD', IF(sla >= rc.min_promised_sla AND sla <= rc.max_promised_sla AND ( s.shipment_priority_flag LIKE 'NDD%' OR s.shipment_priority_flag LIKE 'SDD%'), (IF(s.shipment_priority_flag LIKE 'NDD%', IF(s.air_flag = 1,rc.ndd_charge_air,rc.ndd_charge) , 0 ) + IF(s.shipment_priority_flag LIKE 'SDD%', rc.sdd_charge, 0 ) - rc.forward_rate ), 0 ), 0 ) AS priority_shipment_incremental_revenue,
                                           IF(shipment_carrier = 'FSD', IF(s.ekl_shipment_type = 'rvp', rc.rvp_rate, 0 ), 0 ) AS rvp_revenue,
                                           IF(shipment_carrier = 'FSD', IF(lower(s.reverse_shipment_type) = 'replacement' AND s.ekl_shipment_type = 'rvp',10,0), 0) AS replacement_discount,
                                           --Need to modify the logic,need to fix rate based on no of shipments per day 
                                           IF(shipment_carrier ='FSD', IF(sla >= rc.min_promised_sla AND sla <= 20000, IF(s.seller_type = 'Non-FA' AND s.shipment_current_status <> 'not_received' AND ekl_shipment_type NOT IN ('rvp', 'merchant_return'), IF(z.no_of_daily_first_mile_shipments > 300 , rc.first_mile_rate_300, IF(z.no_of_daily_first_mile_shipments <300 AND z.no_of_daily_first_mile_shipments >70 , rc.first_mile_rate_70_300, rc.first_mile_rate_70 ) ), 0 ), 0 ), 0 ) AS first_mile_revenue,
                                           IF(shipment_carrier ='FSD', IF(sla >= rc.min_promised_sla AND sla <= 20000, IF(s.shipment_fa_flag = 0 AND s.shipment_current_status <> 'not_received' AND ekl_shipment_type NOT IN ('rvp', 'merchant_return'), IF(z.no_of_daily_first_mile_shipments >=300 , "GTE_300", IF(z.no_of_daily_first_mile_shipments <300  AND  z.no_of_daily_first_mile_shipments >70 , "BW_70_300", "LTE_70" ) ), "NOT_APPLICABLE" ), "NOT_APPLICABLE" ), "NOT_APPLICABLE" ) AS first_mile_revenue_flag,
                                           IF(shipment_carrier = '3PL', 10, 0 )  AS handover_revenue_3pl
                          FROM             DEFAULT.ekl_pnl_revenue_rate_card_2_3 rc 
                          RIGHT OUTER JOIN  (        SELECT    t.vendor_tracking_id, 
                                                               shipment_first_received_at_datetime,
                                                               shipment_dispatch_datetime, 
                                                               received_at_origin_facility_datetime,
                                                               --use coalesce here 
                                                               month(IF(shipment_dispatch_datetime IS NULL, IF(shipment_first_received_at_datetime IS NULL, shipment_created_at_datetime,shipment_first_received_at_datetime), shipment_dispatch_datetime)) AS actual_month,
                                                               IF(datediff( to_date( 
                                                               --coalesce(shipment_delivered_at_datetime,logistics_promise_datetime) 
                                                               IF(shipment_delivered_at_datetime IS NULL, logistics_promise_datetime, shipment_delivered_at_datetime ) ), to_date(
                                                               --coalesce(shipment_dispatch_datetime,shipment_first_received_at_datetime,shipment_created_at_datetime)
                                                               IF(shipment_dispatch_datetime IS NULL, IF(shipment_first_received_at_datetime IS NULL, shipment_created_at_datetime, shipment_first_received_at_datetime ), shipment_dispatch_datetime ) ) )<0, 3, datediff( to_date( 
                                                               --coalesce(shipment_delivered_at_datetime,logistics_promise_datetime) 
                                                               IF(shipment_delivered_at_datetime IS NULL, logistics_promise_datetime, shipment_delivered_at_datetime ) ), to_date(
                                                               --coalesce(shipment_dispatch_datetime,shipment_first_received_at_datetime,shipment_created_at_datetime)
                                                               IF(shipment_dispatch_datetime IS NULL, IF(shipment_first_received_at_datetime IS NULL, shipment_created_at_datetime, shipment_first_received_at_datetime ) ,shipment_dispatch_datetime ) ) ) ) AS sla,
                                                               ekl_shipment_type, 
                                                               shipment_priority_flag, 
                                                               payment_type, 
                                                               payment_mode, 
                                                               shipment_value, 
                                                               shipment_fa_flag, 
                                                               shipment_current_status, 
                                                               seller_id, 
                                                               logistics_promise_datetime, 
                                                               ekl_fin_zone, 
                                                               seller_type, 
                                                               IF(p.billable_weight IS NULL OR p.billable_weight = 0, IF(substr(t.vendor_tracking_id,1,3)='FMP', 850, 1400 ), IF(p.billable_weight <=15000, p.billable_weight, 10000 )) AS billable_weight,
                                                               floor( IF(p.billable_weight IS NULL OR p.billable_weight = 0, IF(substr(t.vendor_tracking_id,1,3)='FMP', 850, 1400 ), IF(p.billable_weight <=15000, p.billable_weight, 10000 )) /500)*500 AS weight_bucket,
                                                               -- If(P.billable_weight <=15000, 
                                                               --     Floor(If(shipment_fa_flag = 0, 
                                                               --             if(if(shipment_weight=0,1,shipment_weight)-1 >((if(billable_weight=0,1,billable_weight)-1)*0.8), 
                                                               --                 if(shipment_weight=0,1,shipment_weight)-1, 
                                                               --                 (if(billable_weight=0,1,billable_weight)-1)*0.8 
                                                               --               ), 
                                                               --             (if(billable_weight=0,1,billable_weight)-1))/500 
                                                               --           )*500, 
                                                               --         10000 
                                                               --       ) AS weight_bucket, 
                                                               IF(shipment_value >20000, "ASP>20K", IF(shipment_value>15000, "ASP_15K_to_20K", IF(shipment_value>10000, "ASP_10K_to_15K", IF(shipment_value>5000, "ASP_5K_to_10K", "ASP<5K" ) ) ) ) AS asp_bucket,
                                                               shipment_carrier, 
                                                               source_city, 
                                                               source_pincode, 
                                                               source_state, 
                                                               destination_zone, 
                                                               destination_city, 
                                                               destination_pincode, 
                                                               destination_state, 
                                                               ekart_lzn_flag, 
                                                               reverse_shipment_type, 
                                                               h.air_flag 
                                                     FROM      ( 
                                                                      SELECT vendor_tracking_id,
                                                                             merchant_reference_id,
                                                                             seller_id, 
                                                                             shipment_current_status,
                                                                             payment_type, 
                                                                             payment_mode, 
                                                                             amount_collected,
                                                                             seller_type, 
                                                                             shipment_dg_flag,
                                                                             shipment_fragile_flag,
                                                                             shipment_priority_flag,
                                                                             service_tier, 
                                                                             surface_mandatory_flag,
                                                                             ekl_shipment_type,
                                                                             reverse_shipment_type,
                                                                             ekl_fin_zone, 
                                                                             ekart_lzn_flag, 
                                                                             shipment_fa_flag,
                                                                             vendor_id, 
                                                                             shipment_carrier,
                                                                             shipment_weight,
                                                                             sender_weight, 
                                                                             system_weight, 
                                                                             logistics_promise_datetime,
                                                                             volumetric_weight_source,
                                                                             volumetric_weight,
                                                                             billable_weight,
                                                                             billable_weight_type,
                                                                             shipment_value, 
                                                                             cod_amount_to_collect,
                                                                             shipment_charge,
                                                                             source_address_pincode,
                                                                             destination_address_pincode,
                                                                             fsd_assigned_hub_id,
                                                                             reverse_pickup_hub_id,
                                                                             shipment_created_at_datetime,
                                                                             shipment_dispatch_datetime,
                                                                             vendor_dispatch_datetime,
                                                                             shipment_first_received_at_datetime,
                                                                             shipment_delivered_at_datetime,
                                                                             received_at_origin_facility_datetime,
                                                                             rto_create_datetime,
                                                                             rto_complete_datetime
                                                                      FROM   bigfoot_external_neo.scp_ekl__shipment_l1_90_fact
                                                                      WHERE  length(vendor_tracking_id) > 5
                                                                      AND    shipment_carrier IN ('3PL', 'FSD') 
																	  ) t
                                                     LEFT JOIN 
                                                               ( 
                                                                      SELECT vendor_tracking_id,
                                                                             IF (shipment_dead_weight > volumetric_weight, shipment_dead_weight,volumetric_weight) AS billable_weight
                                                                      FROM   bigfoot_external_neo.scp_ekl__fc_profiler_volumetric_estimate_final_hive_fact) p
                                                     ON        p.vendor_tracking_id = t.vendor_tracking_id
                                                     LEFT JOIN 
                                                               ( 
                                                                      SELECT sh.vendor_tracking_id             AS tracking_id,
                                                                             IF(sh.number_of_air_hops > 0,1,0) AS air_flag
                                                                      FROM   bigfoot_external_neo.scp_ekl__shipment_hive_90_fact sh
                                                                      WHERE  sh.shipment_carrier = 'FSD' ) h
                                                     ON        h.tracking_id = t.vendor_tracking_id
                                                     LEFT JOIN 
                                                               ( 
                                                                      SELECT pincode AS destination_pincode,
                                                                             city    AS destination_city,
                                                                             state   AS destination_state,
                                                                             zone    AS destination_zone
                                                                      FROM   bigfoot_external_neo.scp_ekl__logistics_geo_hive_dim) u
                                                               --rvp use source pincode 
                                                     ON        IF(ekl_shipment_type = 'rvp',t.source_address_pincode,t.destination_address_pincode) = u.destination_pincode
                                                     LEFT JOIN 
                                                               ( 
                                                                      SELECT pincode AS source_pincode,
                                                                             city    AS source_city,
                                                                             state   AS source_state
                                                                      FROM   bigfoot_external_neo.scp_ekl__logistics_geo_hive_dim )v
                                                     ON        IF(ekl_shipment_type = 'rvp',t.destination_address_pincode,t.source_address_pincode) = v.source_pincode
                                                     WHERE     t.vendor_tracking_id <> 'not_assigned' 
						                    ) s
                                           --zones names as is from rate card 
                          ON    ( CASE 
									 WHEN s.ekl_fin_zone = 'INTRACITY' THEN 'LOCAL'
									 WHEN s.ekl_fin_zone IN('Missing',
															'INTRAZONE') THEN 'ZONAL'
									 WHEN ( s.source_city IN ('CHENNAI','MUMBAI','NEW DELHI','KOLKATA','BANGALORE','HYDERABAD','AHMEDABAD','PUNE')
													  AND s.destination_city IN ('CHENNAI','MUMBAI','NEW DELHI','KOLKATA','BANGALORE','HYDERABAD','AHMEDABAD','PUNE')
													  AND              s.source_city <>s.destination_city)
													  --need to check sla value with pavan 
											 THEN IF(s.sla<4,'METRO_EXPRESS','METRO_NONEXPRESS')
									 WHEN s.destination_state IN ('SIKKIM','ASSAM','MANIPUR','MEGHALAYA','MIZORAM','ARUNACHAL PRADESH','NAGALAND','TRIPURA','JAMMU AND KASHMIR') 
											 THEN 'JK_NE'
									 WHEN s.ekart_lzn_flag = 'ROI' AND s.destination_zone IN ('SOUTH','WEST') 
											 THEN IF(s.sla<4 ,'ROI_EXPRESS','ROI_STANDARD')
									 WHEN s.ekart_lzn_flag = 'ROI' AND s.destination_zone IN ('EAST','NORTH') 
											 THEN 'ROI_BLENDED'
									 ELSE 'ROI_BLENDED'
                                  END
								) = rc.lzn 
                          AND s.actual_month = rc.rate_card_month 
                          AND s.weight_bucket = rc.weight_bucket_min 
                          LEFT OUTER JOIN 
                                           (    SELECT   seller_id, 
                                                             --use recieved at origin date time 
                                                             to_date(received_at_origin_facility_datetime) AS received_at_origin_facility_date,
                                                             --shipment_received_at_date, 
                                                             count(vendor_tracking_id) AS no_of_daily_first_mile_shipments
                                                    FROM     bigfoot_external_neo.scp_ekl__shipment_l1_90_fact
                                                    WHERE    seller_type='Non-FA' 
                                                    AND      shipment_carrier = 'FSD' 
                                                    GROUP BY seller_id, 
                                                             to_date(received_at_origin_facility_datetime)) z
                          ON s.seller_id = z.seller_id AND to_date(s.received_at_origin_facility_datetime) = z.received_at_origin_facility_date ) x
GROUP BY x.vendor_tracking_id, 
         x.shipment_fa_flag, 
         x.seller_id, 
         x.lzn, 
         x.asp_bucket, 
         lookup_date(x.shipment_dispatch_datetime), 
         lookup_time(x.shipment_dispatch_datetime), 
         lookup_date(x.shipment_first_received_at_datetime), 
         lookup_time(x.shipment_first_received_at_datetime), 
         lookup_date(x.received_at_origin_facility_datetime), 
         lookup_time(x.received_at_origin_facility_datetime), 
         x.shipment_value, 
         x.billable_weight, 
         IF(x.sla < 4,'<= 3 Days','> 3 Days'), 
         concat(x.weight_bucket,'-',(x.weight_bucket+500)), 
         x.first_mile_revenue_flag, 
         lookupkey('pincode',x.source_pincode), 
         lookupkey('pincode',x.destination_pincode), 
         x.air_flag, 
         x.reverse_shipment_type, 
         x.ekart_lzn_flag ;
		 