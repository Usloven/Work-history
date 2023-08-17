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
		
		
		
		
		r20 AS (
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
            'Раздел 2.0 Сбросы'::text AS type,
            sum(cal.actual_em) AS actual_em,
            sum(cal.actual_em_nds) AS actual_em_nds,
            sum(cal.actual_em_vrs) AS actual_em_vrs,
            sum(cal.actual_overlimit) AS actual_overlimit,
            sum(cal.sum_nds) AS sum_nds,
            sum(cal.sum_vrs) AS sum_vrs,
            sum(cal.sum_overlimit) AS sum_overlimit,
            sum(cal.sum_all) AS sum_all,
			COALESCE(cal.established_em_nds, 0) as established_em_nds,
			COALESCE(cal.established_em_vrs, 0) as established_em_vrs,
			COALESCE(cal.cost, 0) as cost,
			COALESCE(cal.ratio_knd, 0)as ratio_knd,
			COALESCE(cal.ratio_kvr, 0) as ratio_kvr,
			COALESCE(cal.ratio_overlimit, 0) as ratio_overlimit,
			COALESCE(cal.ratio_kp, 0) as ratio_kp,
			COALESCE(cal.ratio_kot, 0) as ratio_kot,
			COALESCE(cal.ratio_kind, 0) as ratio_kind,
			COALESCE(cal.ratio_kvo, 0) as ratio_kvo
			



           FROM rpn_service_requests req
             JOIN (dpnvos_r20 r20
             JOIN (dpnvos_r20_onvoses onv
             JOIN (dpnvos_r20_sources sou
             JOIN (dpnvos_r20_calcs cal
             LEFT JOIN nsi_pollutants pol ON cal.pollutant_id = pol.id AND pol.deleted_at IS NULL) ON sou.id = cal.source_id AND cal.deleted_at IS NULL) ON doc.id = sou.document_id AND sou.deleted_at IS NULL) ON onv.id = doc.onvos_id AND doc.deleted_at IS NULL) ON r20.id = onv.r20_id AND onv.deleted_at IS NULL) ON req.id = r20.service_request_id AND r20.deleted_at IS NULL AND r20.enabled = true
            
          WHERE req.deleted_at IS NULL AND req.state_service_id = 610 AND (req.status_code = ANY (ARRAY['to_review'::text, 'review'::text, 'accepted'::text, 'paper_waiting'::text, 'declined'::text, 'to_canceled'::text, 'canceled'::text, 'transfered'::text])) AND req.report_year =  2022 AND req.territory_org_id = 15
          GROUP BY req.id, onv.id, onv.onv_id, (
                CASE
                    WHEN onv.onv_id IS NULL THEN 'Отсутствует в реестре НВОС'::text
                    ELSE 'Присутствует в реестре НВОС'::text
                END), onv.name, (
                CASE
                    WHEN onv.category IS NULL OR (onv.category = ANY (ARRAY[5, 0])) THEN 'Некатегорийный объект'::text
                    ELSE onv.category::text
                END), (COALESCE(onv.oktmo, ''::text)), pol.code, cal.code, pol.name, cal.name, pol.hazard_class, 
			cal.established_em_nds,
			cal.established_em_vrs,
			cal.cost,
			cal.ratio_knd,
			cal.ratio_kvr,
			cal.ratio_overlimit,
			cal.ratio_kp,
			cal.ratio_kot,
			cal.ratio_kind,
			cal.ratio_kvo
        ), 
		
		c20 AS (
         SELECT onv.service_request_id,
            onv.id AS onvos_report_id,
			cor.established_em_nds,
			cor.established_em_vrs,
			cor.cost,
			cor.ratio_knd,
			cor.ratio_kvr,
			cor.ratio_overlimit,
			cor.ratio_kp,
			cor.ratio_kot,
			cor.ratio_kind,
			sum(cor.events_sum) AS events_sum,
            sum(cor.sum_with_correction) AS sum_with_correction,
            COALESCE(pol.code, cor.code) AS pollutant_code,
            COALESCE(pol.name, cor.name) AS pollutant_name
           FROM dpnvos_r20_corrections cor
             JOIN dpnvos_r20_documents doc ON cor.document_id = doc.id AND doc.deleted_at IS NULL
             JOIN dpnvos_r20_onvoses onv ON doc.onvos_id = onv.id AND onv.deleted_at IS NULL
             JOIN dpnvos_r20 r20 ON onv.r20_id = r20.id AND r20.enabled = true
             LEFT JOIN nsi_pollutants pol ON cor.pollutant_id = pol.id AND pol.deleted_at IS NULL
             JOIN rpn_service_requests req ON cor.service_request_id = req.id
          WHERE (req.status_code = ANY (ARRAY['to_review'::text, 'review'::text, 'accepted'::text, 'paper_waiting'::text, 'declined'::text, 'to_canceled'::text, 'canceled'::text])) AND req.report_year = 2022 AND cor.deleted_at IS NULL
          GROUP BY onv.id, pol.code, cor.code, pol.name, cor.name, established_em_nds,
			cor.established_em_vrs,
			cor.cost,
			cor.ratio_knd,
			cor.ratio_kvr,
			cor.ratio_overlimit,
			cor.ratio_kp,
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
            r20.req_r_id,
            r20.onvs_report_id,
            r20.onvs_id,
            r20.onvs_number,
            r20.onvs_name,
            r20.onvs_category,
            r20.in_reestr,
            r20.onvs_oktmo,
            r20.pollutant_code,
            r20.pollutant_name,
            r20.hazard_class,
            r20.type,
            r20.actual_em,
            r20.actual_em_nds,
            r20.actual_em_vrs,
            r20.actual_overlimit,
            r20.sum_nds,
            r20.sum_vrs,
            r20.sum_overlimit,
            r20.sum_all,
           	COALESCE(c20.established_em_nds, r20.established_em_nds) as established_em_nds,
			COALESCE(c20.established_em_vrs, r20.established_em_vrs) as established_em_vrs,
			COALESCE(c20.cost, r20.cost) as cost,
			COALESCE(c20.ratio_knd, r20.ratio_knd) as ratio_knd,
			COALESCE(c20.ratio_kvr, r20.ratio_kvr) as ratio_kvr,
			COALESCE(c20.ratio_overlimit, r20.ratio_overlimit) as ratio_overlimit,
			COALESCE(c20.ratio_kp, r20.ratio_kp) as ratio_kp,
			COALESCE(c20.ratio_kot, r20.ratio_kot) as ratio_kot,
			COALESCE(c20.ratio_kind, r20.ratio_kind) as ratio_kind,
			r20.ratio_kvo,

            LEAST(COALESCE(c20.events_sum, 0::double precision), r20.sum_all::double precision) AS sum_events,
            r20.sum_all::double precision - LEAST(COALESCE(c20.events_sum, 0::double precision), r20.sum_all::double precision) AS sum_budget
           FROM r20
             JOIN report ON report.req_id = r20.req_r_id
             LEFT JOIN c20 ON c20.onvos_report_id = r20.onvs_report_id AND r20.pollutant_code = c20.pollutant_code
             LEFT JOIN onvs ON r20.onvs_number = onvs.number
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
    res.hazard_class,
	

-- Установленные сбросы (тонн), НДС, ТН
-- Установленные сбросы (тонн), ВРС
	res.established_em_nds,
	res.established_em_vrs,
	
    res.actual_em,
    res.actual_em_nds,
    res.actual_em_vrs,
    res.actual_overlimit,
	
-- Cтавка платы (руб./тонна)
-- Коэффициент к ставке платы за выброс в пределах НДС, ТН (Кнд)
-- Коэффициент к ставке платы за выброс в пределах ВРС (Квр)
-- Коэффициент к ставке платы за выброс сверх ВРВ, НДВ, ТН (Кср / Кпр)
-- Коэффициент пересчета ставки платы по взвешенным веществам (Кп)
-- Дополнительный коэффициент (Кот)
-- Дополнительный коэффициент (Кво)
-- Поправочный коэффициент (Кинд)

	res.cost,
	res.ratio_knd,
	res.ratio_kvr,
	res.ratio_overlimit,
	res.ratio_kp,
	res.ratio_kot,
	res.ratio_kind,
	res.ratio_kvo,

    res.sum_nds,
    res.sum_vrs,
    res.sum_overlimit,
    res.sum_all,
    res.sum_events,
    res.sum_budget

  FROM res
  ORDER BY res.report_year DESC, res.req_id, res.org_name, res.onvs_number, res.onvs_name;