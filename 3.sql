 WITH report AS (
         SELECT req.id AS req_id,
            req.report_year,
            sta.name AS status,
            COALESCE(to_char(req.sent_at, 'DD.MM.YYYY'::text), ''::text) AS send_date,
                CASE
                    WHEN req.doc_type = '1'::text THEN 'Первичный'::text
                    ELSE 'Уточненный'::text
                END AS doc_vid,
            ter.short_name AS ter_name,
            reg.name AS region,
            COALESCE(app.short_name_organization, app.full_name_organization) AS org_name,
            app.inn,
	 		req.doc_type,
	 		req.parent_id
           	
           
         
           FROM rpn_service_requests req
             JOIN rpn_service_applicant_information app ON req.id = app.service_request_id AND app.deleted_at IS NULL
             JOIN nsi_statuses sta ON req.status_id = sta.id
             JOIN nsi_territory_orgs ter ON req.territory_org_id = ter.id
             LEFT JOIN nsi_regions reg ON req.region_id = reg.id
          WHERE req.state_service_id = 610 AND (req.status_code = ANY (ARRAY['to_review'::text, 'review'::text, 'accepted'::text, 'paper_waiting'::text, 'declined'::text, 'to_canceled'::text, 'canceled'::text, 'transfered'::text])) AND req.report_year =  2022 AND req.territory_org_id = 15
        ),
		
		
		
		
		r30 AS (
         SELECT req.id AS req_r_id,
            onv.id AS onvs_report_id,
            COALESCE(onv.onv_id, 0::bigint) AS onvs_id,
            COALESCE(onv.number, ''::text) AS onvs_number,
            onv.name AS onvs_name,
                CASE
                    WHEN onv.category IS NULL OR (onv.category = ANY (ARRAY[5, 0])) THEN 'Некатегорийный объект'::text
                    ELSE onv.category::text
                END AS onvs_category,
                CASE
                    WHEN onv.onv_id IS NULL THEN 'Отсутствует в реестре НВОС'::text
                    ELSE 'Присутствует в реестре НВОС'::text
                END AS in_reestr,
            cal.id AS cal_id,
            fkko.id AS fkko_id,
            fkko.code AS fkko_code,
            fkko.name AS fkko_name,
            clas.name AS class_name,
			COALESCE(cal.established_limit, 0) as established_limit,
            sum(cal."all") AS formed_reporting_period,
            sum(cal.utilized) AS utilized_reporting_period,
            sum(cal.neutralized) AS neutralized_reporting_period,
            sum(cal.actual_prev_period) AS actual_prev_period,
            sum(cal.actual_balance) AS actual_balance,
            sum(cal.transferred) AS transferred,
            sum(cal.place_in_period) AS place_in_period,
            sum(cal.underlimit) AS including_underlimit,
            sum(cal.overlimit) AS including_overlimit,
            sum(cal.sum_underlimit) AS sum_underlimit,
            sum(cal.sum_overlimit) AS sum_overlimit,
            sum(cal.sum_all) AS sum_all,
			
            
            COALESCE(cal.cost, 0) as cost,
            COALESCE(cal.ratio_kisp, 0) as ratio_kisp,
            COALESCE(cal.ratio_kl, 0) as ratio_kl,
            COALESCE(cal.ratio_ksl, 0) as ratio_ksl,
            COALESCE(cal.ratio_kod, 0) as ratio_kod,
            COALESCE(cal.ratio_kpo, 0) as ratio_kpo,
            COALESCE(cal.ratio_kst, 0) as ratio_kst,
            COALESCE(cal.ratio_kot, 0) as ratio_kot,
            COALESCE(cal.ratio_kind, 0) as ratio_kind

			

             FROM dpnvos_r30 r30
             JOIN dpnvos_r30_onvoses onv ON r30.id = onv.r30_id AND onv.deleted_at IS NULL AND r30.deleted_at IS NULL AND r30.enabled = true
             LEFT JOIN dpnvos_r30_documents doc ON doc.onvos_id = onv.id AND doc.deleted_at IS NULL
             LEFT JOIN dpnvos_r30_calcs cal ON cal.document_id = doc.id AND cal.deleted_at IS NULL
             LEFT JOIN nsi_fkkos fkko ON cal.fkko_id = fkko.id
             LEFT JOIN nsi_waste_hazard_classes clas ON cal.waste_hazard_class_id = clas.id
             JOIN rpn_service_requests req ON req.id = r30.service_request_id AND req.deleted_at IS NULL
            

           
             
          WHERE req.state_service_id = 610 AND (req.status_code = ANY (ARRAY['to_review'::text, 'review'::text, 'accepted'::text, 'paper_waiting'::text, 'declined'::text, 'to_canceled'::text, 'canceled'::text, 'transfered'::text])) AND req.report_year =  2022 AND req.territory_org_id = 15
          

            GROUP BY req.id, onv.id, (COALESCE(onv.number, ''::text)), (
                CASE
                    WHEN onv.category IS NULL OR (onv.category = ANY (ARRAY[5, 0])) THEN 'Некатегорийный объект'::text
                    ELSE onv.category::text
                END), (COALESCE(onv.oktmo, ''::text)), onv.name, (
                CASE
                    WHEN onv.onv_id IS NULL THEN 'Отсутствует в реестре НВОС'::text
                    ELSE 'Присутствует в реестре НВОС'::text
                END), cal.id, fkko.id, fkko.code, fkko.name, clas.name,

 				cal.established_limit,
                cal.underlimit,
                cal.overlimit,
                cal.cost,
                cal.ratio_kisp,
                cal.ratio_kl,
                cal.ratio_ksl,
                cal.ratio_kod,
                cal.ratio_kpo,
                cal.ratio_kst,
                cal.ratio_kot,
                cal.ratio_kind

        ), 
		
		c30 AS (
         SELECT onv.service_request_id,
            onv.id AS onvos_id,
			cor.underlimit,
            cor.overlimit,
            cor.cost,
            cor.ratio_kisp,
            cor.ratio_kl,
            cor.ratio_ksl,
            cor.ratio_kod,
            cor.ratio_kpo,
            cor.ratio_kst,
            cor.ratio_kot,
            cor.ratio_kind,
			cor.established_limit,
			sum(cor.events_sum) AS events_sum,
            sum(cor.sum_with_correction) AS sum_with_correction,
            fkko.id AS fkko_id,
            fkko.code AS fkko_code,
            fkko.name AS fkko_name
           FROM dpnvos_r30_onvoses onv
             LEFT JOIN dpnvos_r30_documents doc ON doc.onvos_id = onv.id AND onv.deleted_at IS NULL AND doc.deleted_at IS NULL
             LEFT JOIN dpnvos_r30_corrections cor ON cor.document_id = doc.id AND cor.deleted_at IS NULL
             LEFT JOIN nsi_fkkos fkko ON cor.fkko_id = fkko.id
             LEFT JOIN nsi_waste_hazard_classes clas ON cor.waste_hazard_class_id = clas.id
             JOIN rpn_service_requests req ON onv.service_request_id = req.id AND req.deleted_at IS NULL
             
          WHERE (req.status_code = ANY (ARRAY['to_review'::text, 'review'::text, 'accepted'::text, 'paper_waiting'::text, 'declined'::text, 'to_canceled'::text, 'canceled'::text])) AND req.report_year = 2022 AND cor.deleted_at IS NULL
          GROUP BY onv.id, cor.code, cor.name,
			fkko.id,
            fkko.code,
            fkko.name,
			cor.established_limit,
            cor.underlimit,
            cor.overlimit,
            cor.cost,
            cor.ratio_kisp,
            cor.ratio_kl,
            cor.ratio_ksl,
            cor.ratio_kod,
            cor.ratio_kpo,
            cor.ratio_kst,
            cor.ratio_kot,
            cor.ratio_kind
			
        ), 
		
		res AS (
         SELECT report.req_id,
            report.report_year,
            report.status,
            report.send_date,
            report.doc_vid,
            report.ter_name,
            report.region,
            report.org_name,
            report.inn,  
			report.doc_type,
	 		report.parent_id,
			
            r30.req_r_id,
            r30.onvs_report_id,
            r30.onvs_id,
            r30.onvs_number,
            r30.onvs_name,
            r30.onvs_category,
            r30.in_reestr,
			
			
			r30.formed_reporting_period,
			r30.utilized_reporting_period,
			r30.neutralized_reporting_period,
			r30.actual_prev_period,
			r30.actual_balance,
			r30.transferred,
			r30.place_in_period,
			r30.including_underlimit,
			r30.including_overlimit,
			r30.sum_underlimit,
			r30.sum_overlimit,
			r30.sum_all,
			
			
     
			r30.fkko_id,
            r30.fkko_code,
            r30.fkko_name,
            r30.class_name,
			COALESCE(c30.established_limit, r30.established_limit) as established_limit,
           	COALESCE(c30.underlimit, r30.including_underlimit) as underlimit,
            COALESCE(c30.overlimit, r30.including_overlimit) as overlimit,
            COALESCE(c30.cost, r30.cost) as cost,
            COALESCE(c30.ratio_kisp, r30.ratio_kisp) as ratio_kisp,
            COALESCE(c30.ratio_kl, r30.ratio_kl) as ratio_kl,
            COALESCE(c30.ratio_ksl, r30.ratio_ksl) as ratio_ksl,
            COALESCE(c30.ratio_kod, r30.ratio_kod) as ratio_kod,
            COALESCE(c30.ratio_kpo, r30.ratio_kpo) as ratio_kpo,
            COALESCE(c30.ratio_kst, r30.ratio_kst) as ratio_kst,
            COALESCE(c30.ratio_kot, r30.ratio_kot) as ratio_kot,
            COALESCE(c30.ratio_kind, r30.ratio_kind) as ratio_kind,


            LEAST(COALESCE(c30.events_sum, 0::double precision), r30.sum_all::double precision) AS sum_events,
            r30.sum_all::double precision - LEAST(COALESCE(c30.events_sum, 0::double precision), r30.sum_all::double precision) AS sum_budget
           FROM r30
             JOIN report ON report.req_id = r30.req_r_id
             LEFT JOIN c30 ON c30.onvos_id = r30.onvs_id AND r30.fkko_code = c30.fkko_code
             LEFT JOIN onvs ON r30.onvs_number = onvs.number
			inner join rpn.dpnvos_r20 r20 on report.req_id = r20.service_request_id and r20.enabled = True and  r20.deleted_at is Null
        )
		
		
 SELECT row_number() OVER (ORDER BY res.report_year DESC, res.req_id, res.org_name, res.onvs_number, res.onvs_name) AS id,
    res.onvs_number,
	res.onvs_name,
	res.inn,
	res.org_name,
	res.req_r_id,
	res.doc_vid,
	res.doc_type,
	res.parent_id,
	res.send_date,
    
    res.status,
    res.ter_name,
    res.region,
	res.report_year,
    res.fkko_code,
    res.fkko_name,
    res.class_name,
	res.established_limit,
    res.formed_reporting_period,
    res.utilized_reporting_period,
    res.neutralized_reporting_period,
    res.actual_prev_period,
    res.actual_balance,
    res.transferred,
    res.place_in_period,
    res.including_underlimit,
    res.including_overlimit,
	res.cost,
	res.ratio_kisp,
	res.ratio_kl,
	res.ratio_ksl,
	res.ratio_kod,
	res.ratio_kpo,
	res.ratio_kst,
	res.ratio_kot,
	res.ratio_kind,

	
	
    res.sum_underlimit,
    res.sum_overlimit,
    res.sum_all,
    res.sum_events,
    res.sum_budget

  FROM res
  ORDER BY res.report_year DESC, res.req_id, res.org_name, res.onvs_number, res.onvs_name;