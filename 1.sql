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
		
		
		
		
		r10 AS (
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
            COALESCE(onv.oktmo, ''::text) AS onvs_oktmo,
            COALESCE(pol.code, cal.code) AS pollutant_code,
            COALESCE(pol.name, cal.name) AS pollutant_name,
            pol.hazard_class,
            'Раздел 1.0 Выбросы'::text AS type,
            sum(cal.actual_em) AS actual_em,
            sum(cal.actual_em_pvd) AS actual_em_pvd,
            sum(cal.actual_em_vsv) AS actual_em_vsv,
            sum(cal.actual_overlimit) AS actual_overlimit,
            sum(cal.sum_pvd) AS sum_pvd,
            sum(cal.sum_vsv) AS sum_vsv,
            sum(cal.sum_overlimit) AS sum_overlimit,
            sum(cal.sum_all) AS sum_all,
            0 AS sum_all_png
			COALESCE(cal.established_em_pvd, 0) as established_em_pvd,
			COALESCE(cal.established_em_vsv, 0) as established_em_vsv,
			COALESCE(cal.cost, 0) as cost,
			COALESCE(cal.ratio_knd, 0) as ratio_knd,
			COALESCE(cal.ratio_kvr, 0) as ratio_kvr,
			COALESCE(cal.ratio_overlimit, 0) as ratio_overlimit,
			COALESCE(cal.ratio_kot, 0) as ratio_kot,
			COALESCE(cal.ratio_kind, 0) as ratio_kind


           FROM rpn_service_requests req
             JOIN (dpnvos_r10 r10
             JOIN (dpnvos_r10_onvoses onv
             JOIN (dpnvos_r10_documents doc
             JOIN (dpnvos_r10_sources sou
             JOIN (dpnvos_r10_calcs cal
             LEFT JOIN nsi_pollutants pol ON cal.pollutant_id = pol.id AND pol.deleted_at IS NULL) ON sou.id = cal.source_id AND cal.deleted_at IS NULL) ON doc.id = sou.document_id AND sou.deleted_at IS NULL) ON onv.id = doc.onvos_id AND doc.deleted_at IS NULL) ON r10.id = onv.r10_id AND onv.deleted_at IS NULL) ON req.id = r10.service_request_id AND r10.deleted_at IS NULL AND r10.enabled = true
             
          WHERE req.deleted_at IS NULL AND req.state_service_id = 610 AND (req.status_code = ANY (ARRAY['to_review'::text, 'review'::text, 'accepted'::text, 'paper_waiting'::text, 'declined'::text, 'to_canceled'::text, 'canceled'::text, 'transfered'::text])) AND req.report_year =  2022 AND req.territory_org_id = 15
          GROUP BY req.id, onv.id, onv.onv_id, (
                CASE
                    WHEN onv.onv_id IS NULL THEN 'Отсутствует в реестре НВОС'::text
                    ELSE 'Присутствует в реестре НВОС'::text
                END), onv.name, (
                CASE
                    WHEN onv.category IS NULL OR (onv.category = ANY (ARRAY[5, 0])) THEN 'Некатегорийный объект'::text
                    ELSE onv.category::text
                END), (COALESCE(onv.oktmo, ''::text)), mun.name, pol.code, cal.code, pol.name, cal.name, pol.hazard_class
        ), 
		
		
		r11 AS (
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
            COALESCE(onv.oktmo, ''::text) AS onvs_oktmo,
            mun.name AS mun_name,
            COALESCE(pol.code, cal.code) AS pollutant_code,
            COALESCE(pol.name, cal.name) AS pollutant_name,
            pol.hazard_class,
            'Раздел 1.1 Выбросы ПНГ в пределах'::text AS type,
            sum(cal.actual_em) AS actual_em,
            sum(cal.actual_em_pvd) AS actual_em_pvd,
            sum(cal.actual_em_vsv) AS actual_em_vsv,
            sum(cal.actual_overlimit) AS actual_overlimit,
            sum(cal.sum_pvd) AS sum_pvd,
            sum(cal.sum_vsv) AS sum_vsv,
            sum(cal.sum_overlimit) AS sum_overlimit,
            0 AS sum_all,
            sum(cal.sum_all) AS sum_all_png,
			COALESCE(cal.established_em_pvd, 0) as established_em_pvd,
			COALESCE(cal.established_em_vsv, 0) as established_em_vsv,
			COALESCE(cal.cost, 0) as cost,
			COALESCE(cal.ratio_knd, 0) as ratio_knd,
			COALESCE(cal.ratio_kvr, 0) as ratio_kvr,
			COALESCE(cal.ratio_overlimit, 0) as ratio_overlimit,
			COALESCE(cal.ratio_kot, 0) as ratio_kot,
			COALESCE(cal.ratio_kind, 0) as ratio_kind
           FROM rpn_service_requests req
             JOIN (dpnvos_r11 r11
             JOIN (dpnvos_r11_onvoses onv
             JOIN (dpnvos_r11_documents doc
             JOIN (dpnvos_r11_sources sou
             JOIN (dpnvos_r11_calcs cal
             LEFT JOIN nsi_pollutants pol ON cal.pollutant_id = pol.id AND pol.deleted_at IS NULL) ON sou.id = cal.source_id AND cal.deleted_at IS NULL) ON doc.id = sou.document_id AND sou.deleted_at IS NULL) ON onv.id = doc.onvos_id AND doc.deleted_at IS NULL) ON r11.id = onv.r11_id AND onv.deleted_at IS NULL) ON req.id = r11.service_request_id AND r11.deleted_at IS NULL AND r11.enabled = true
             LEFT JOIN nsi_municipalities mun ON sou.oktmo = mun.oktmo AND mun.deleted_at IS NULL
          WHERE req.deleted_at IS NULL AND req.state_service_id = 610 AND (req.status_code = ANY (ARRAY['to_review'::text, 'review'::text, 'accepted'::text, 'paper_waiting'::text, 'declined'::text, 'to_canceled'::text, 'canceled'::text])) AND req.report_year > 2019
          GROUP BY req.id, onv.id, onv.onv_id, (
                CASE
                    WHEN onv.onv_id IS NULL THEN 'Отсутствует в реестре НВОС'::text
                    ELSE 'Присутствует в реестре НВОС'::text
                END), onv.name, (
                CASE
                    WHEN onv.category IS NULL OR (onv.category = ANY (ARRAY[5, 0])) THEN 'Некатегорийный объект'::text
                    ELSE onv.category::text
                END), (COALESCE(onv.oktmo, ''::text)), mun.name, pol.code, cal.code, pol.name, cal.name, pol.hazard_class
        ),
		
		
		 r12 AS (
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
            COALESCE(onv.oktmo, ''::text) AS onvs_oktmo,
            mun.name AS mun_name,
            COALESCE(pol.code, cal.code) AS pollutant_code,
            COALESCE(pol.name, cal.name) AS pollutant_name,
            pol.hazard_class,
            'Раздел 1.2 Выбросы ПНГ сверхлимит'::text AS type,
            sum(cal.actual_em) AS actual_em,
			COALESCE(cal.cost, 0) as cost,
			COALESCE(cal.ratio_kot, 0) as ratio_kot,
			COALESCE(cal.ratio_kind, 0) as ratio_kind
            0 AS actual_em_pvd,
            0 AS actual_em_vsv,
            0 AS actual_overlimit,
            0 AS sum_pvd,
            0 AS sum_vsv,
            sum(cal.sum_overlimit) AS sum_overlimit,
            0 AS sum_all,
            sum(cal.sum_all) AS sum_all_png
           FROM rpn_service_requests req
             JOIN (dpnvos_r12 r12
             JOIN (dpnvos_r12_onvoses onv
             JOIN (dpnvos_r12_documents doc
             JOIN (dpnvos_r12_sources sou
             JOIN (dpnvos_r12_calcs cal
             LEFT JOIN nsi_pollutants pol ON cal.pollutant_id = pol.id AND pol.deleted_at IS NULL) ON sou.id = cal.source_id AND cal.deleted_at IS NULL) ON doc.id = sou.document_id AND sou.deleted_at IS NULL) ON onv.id = doc.onvos_id AND doc.deleted_at IS NULL) ON r12.id = onv.r12_id AND onv.deleted_at IS NULL) ON req.id = r12.service_request_id AND r12.deleted_at IS NULL AND r12.enabled = true
             LEFT JOIN nsi_municipalities mun ON sou.oktmo = mun.oktmo AND mun.deleted_at IS NULL
          WHERE req.deleted_at IS NULL AND req.state_service_id = 610 AND (req.status_code = ANY (ARRAY['to_review'::text, 'review'::text, 'accepted'::text, 'paper_waiting'::text, 'declined'::text, 'to_canceled'::text, 'canceled'::text])) AND req.report_year > 2019
          GROUP BY req.id, onv.id, onv.onv_id, (
                CASE
                    WHEN onv.onv_id IS NULL THEN 'Отсутствует в реестре НВОС'::text
                    ELSE 'Присутствует в реестре НВОС'::text
                END), onv.name, (
                CASE
                    WHEN onv.category IS NULL OR (onv.category = ANY (ARRAY[5, 0])) THEN 'Некатегорийный объект'::text
                    ELSE onv.category::text
                END), (COALESCE(onv.oktmo, ''::text)), mun.name, pol.code, cal.code, pol.name, cal.name, pol.hazard_class
        ), c10 AS (
         SELECT onv.service_request_id,
            onv.id AS onvos_report_id,
            sum(cor.events_sum) AS events_sum,
            sum(cor.sum_with_correction) AS sum_with_correction,
            COALESCE(pol.code, cor.code) AS pollutant_code,
            COALESCE(pol.name, cor.name) AS pollutant_name,
			cor.established_em_pvd,
			cor.established_em_vsv,
			cor.cost,
			cor.ratio_knd,
			cor.ratio_kvr,
			cor.ratio_overlimit,
			cor.ratio_kot,
			cor.ratio_kind


           FROM dpnvos_r10_corrections cor
             JOIN dpnvos_r10_documents doc ON cor.document_id = doc.id AND doc.deleted_at IS NULL
             JOIN dpnvos_r10_onvoses onv ON doc.onvos_id = onv.id AND onv.deleted_at IS NULL
             JOIN dpnvos_r10 r10 ON onv.r10_id = r10.id AND r10.enabled = true
             LEFT JOIN nsi_pollutants pol ON cor.pollutant_id = pol.id AND pol.deleted_at IS NULL
             JOIN rpn_service_requests req ON cor.service_request_id = req.id
          WHERE (req.status_code = ANY (ARRAY['to_review'::text, 'review'::text, 'accepted'::text, 'paper_waiting'::text, 'declined'::text, 'to_canceled'::text, 'canceled'::text])) AND req.report_year > 2019 AND cor.deleted_at IS NULL
          GROUP BY onv.id, pol.code, cor.code, pol.name, cor.name,
		cor.established_em_pvd,
		cor.established_em_vsv,
		cor.cost,
		cor.ratio_knd,
		cor.ratio_kvr,
		cor.ratio_overlimit,
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
            report.kpp,
            report.address,
            
            r10.req_r_id,
            r10.onvs_report_id,
            r10.onvs_id,
            r10.onvs_number,
            r10.onvs_name,
            r10.onvs_category,
            r10.in_reestr,
            r10.onvs_oktmo,
            r10.mun_name,
            r10.pollutant_code,
            r10.pollutant_name,
            r10.hazard_class,
            r10.type,
            r10.actual_em,
            r10.actual_em_pvd,
            r10.actual_em_vsv,
            r10.actual_overlimit,
            r10.sum_pvd,
            r10.sum_vsv,
            r10.sum_overlimit,
            r10.sum_all,
            r10.sum_all_png,
			COALESCE(c10.established_em_pvd, r10.established_em_pvd) as established_em_pvd,
			COALESCE(c10.established_em_vsv, r10.established_em_vsv) as established_em_vsv,
			COALESCE(c10.cost, r10.cost) as cost,
			COALESCE(c10.ratio_knd, r10.ratio_knd) as ratio_knd,
			COALESCE(c10.ratio_kvr, r10.ratio_kvr) as ratio_kvr,
			COALESCE(c10.ratio_overlimit, r10.ratio_overlimit) as ratio_overlimit,
			COALESCE(c10.ratio_kot, r10.ratio_kot) as ratio_kot,
			COALESCE(c10.ratio_kind, r10.ratio_kind) as ratio_kind,
            
            LEAST(COALESCE(c10.events_sum, 0::double precision), r10.sum_all::double precision) AS sum_events,
            r10.sum_all::double precision - LEAST(COALESCE(c10.events_sum, 0::double precision), r10.sum_all::double precision) AS sum_budget
           FROM r10
             JOIN report ON report.req_id = r10.req_r_id
             LEFT JOIN c10 ON c10.onvos_report_id = r10.onvs_report_id AND r10.pollutant_code = c10.pollutant_code
             LEFT JOIN onvs ON r10.onvs_number = onvs.number
        UNION
         SELECT report.req_id,
            report.report_year,
            report.status,
            report.send_date,
            report.doc_vid,
            report.ter_name,
            report.region,
            report.org_name,
            report.inn,
            report.kpp,
            report.address,
           
            r11.req_r_id,
            r11.onvs_report_id,
            r11.onvs_id,
            r11.onvs_number,
            r11.onvs_name,
            r11.onvs_category,
            r11.in_reestr,
            r11.onvs_oktmo,
            r11.mun_name,
            r11.pollutant_code,
            r11.pollutant_name,
            r11.hazard_class,
            r11.type,
            r11.actual_em,
            r11.actual_em_pvd,
            r11.actual_em_vsv,
            r11.actual_overlimit,
            r11.sum_pvd,
            r11.sum_vsv,
            r11.sum_overlimit,
            r11.sum_all,
            r11.sum_all_png,
			COALESCE(c10.established_em_pvd, r11.established_em_pvd) as established_em_pvd,
			COALESCE(c10.established_em_vsv, r11.established_em_vsv) as established_em_vsv,
			COALESCE(c10.cost, r11.cost) as cost,
			COALESCE(c10.ratio_knd, r11.ratio_knd) as ratio_knd,
			COALESCE(c10.ratio_kvr, r11.ratio_kvr) as ratio_kvr,
			COALESCE(c10.ratio_overlimit, r11.ratio_overlimit) as ratio_overlimit,
			COALESCE(c10.ratio_kot, r11.ratio_kot) as ratio_kot,
			COALESCE(c10.ratio_kind, r11.ratio_kind) as ratio_kind,
         
            0 AS sum_events,
            r11.sum_all AS sum_budget
           FROM r11
             JOIN report ON report.req_id = r11.req_r_id
             LEFT JOIN onvs ON r11.onvs_number = onvs.number
        UNION
         SELECT report.req_id,
            report.report_year,
            report.status,
            report.send_date,
            report.doc_vid,
            report.ter_name,
            report.region,
            report.org_name,
            report.inn,
            report.kpp,
            report.address,
            
            r12.req_r_id,
            r12.onvs_report_id,
            r12.onvs_id,
            r12.onvs_number,
            r12.onvs_name,
            r12.onvs_category,
            r12.in_reestr,
            r12.onvs_oktmo,
            r12.mun_name,
            r12.pollutant_code,
            r12.pollutant_name,
            r12.hazard_class,
            r12.type,
            r12.actual_em,
            r12.actual_em_pvd,
            r12.actual_em_vsv,
            r12.actual_overlimit,
            r12.sum_pvd,
            r12.sum_vsv,
            r12.sum_overlimit,
            r12.sum_all,
            r12.sum_all_png,
			
			
			COALESCE(c10.cost, r12.cost) as cost,
			COALESCE(c10.ratio_kot, r12.ratio_kot) as ratio_kot,
			COALESCE(c10.ratio_kind, r12.ratio_kind) as ratio_kind,

            0 AS sum_events,
            r12.sum_all AS sum_budget
           FROM r12
             JOIN report ON report.req_id = r12.req_r_id
             LEFT JOIN onvs ON r12.onvs_number = onvs.number
        )
 SELECT row_number() OVER (ORDER BY res.report_year DESC, res.req_id, res.org_name, res.onvs_number, res.onvs_name) AS id,
    res.onvs_number,
	res.onvs_name,
	res.inn,
	res.org_name,
	res.req_id,
	res.doc_vid,
	res.doc_type,
	res.parent_id,
	res.send_date,
    
    res.status,
    res.ter_name,
    res.region,
	res.report_year,
    res.pollutant_code,
    res.pollutant_name,
    res.established_em_pvd,
	res.established_em_vsv,	
	res.actual_em,
    res.actual_em_pvd,
    res.actual_em_vsv,
    res.actual_overlimit,
	res.cost,
	res.ratio_knd,
	res.ratio_kvr,
	res.ratio_overlimit,
	res.ratio_kot,
	res.ratio_kind,
    res.sum_pvd,
    res.sum_vsv,
    res.sum_overlimit,
    res.sum_all,
    res.sum_all_png,
    res.sum_events,
    res.sum_budget,
    res.sum_all_png AS sum_budget_png
   FROM res
  ORDER BY res.report_year DESC, res.req_id, res.org_name, res.onvs_number, res.onvs_name;