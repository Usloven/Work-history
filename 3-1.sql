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
            
            COALESCE(app.post_address_street, app.legal_address_street) AS address,
            app.okved AS app_okved
           FROM rpn_service_requests req
             JOIN rpn_service_applicant_information app ON req.id = app.service_request_id AND app.deleted_at IS NULL
             JOIN nsi_statuses sta ON req.status_id = sta.id
             JOIN nsi_territory_orgs ter ON req.territory_org_id = ter.id
             LEFT JOIN nsi_regions reg ON req.region_id = reg.id
          WHERE req.state_service_id = 610 AND (req.status_code = ANY (ARRAY['to_review'::text, 'review'::text, 'accepted'::text, 'paper_waiting'::text, 'declined'::text, 'to_canceled'::text, 'canceled'::text])) AND req.report_year > 2019 AND ter.id <> 33 AND ter.id <> 34
        ), 
        
        
        
        
        
        
        
        r30 AS (
         SELECT onv.service_request_id,
            onv.id AS onvos_id,
            COALESCE(onv.number, ''::text) AS onv_number,
            onv.name AS onv_name,
                CASE
                    WHEN onv.category IS NULL OR (onv.category = ANY (ARRAY[5, 0])) THEN 'Некатегорийный объект'::text
                    ELSE onv.category::text
                END AS onv_category,
                CASE
                    WHEN onv.onv_id IS NULL THEN 'Отсутствует в реестре НВОС'::text
                    ELSE 'Присутствует в реестре НВОС'::text
                END AS is_in_reestr,
            COALESCE(onv.oktmo, ''::text) AS onvs_oktmo,
            mun.name AS mun_name,
            cal.id AS cal_id,
            fkko.id AS fkko_id,
            fkko.code AS fkko_code,
            fkko.name AS fkko_name,
            clas.name AS class_name,
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
            sum(cal.sum_all) AS sum_all
           FROM dpnvos_r30 r30
             JOIN dpnvos_r30_onvoses onv ON r30.id = onv.r30_id AND onv.deleted_at IS NULL AND r30.deleted_at IS NULL AND r30.enabled = true
             LEFT JOIN dpnvos_r30_documents doc ON doc.onvos_id = onv.id AND doc.deleted_at IS NULL
             LEFT JOIN dpnvos_r30_calcs cal ON cal.document_id = doc.id AND cal.deleted_at IS NULL
             LEFT JOIN nsi_fkkos fkko ON cal.fkko_id = fkko.id
             LEFT JOIN nsi_waste_hazard_classes clas ON cal.waste_hazard_class_id = clas.id
             JOIN rpn_service_requests req ON req.id = r30.service_request_id AND req.deleted_at IS NULL
             LEFT JOIN nsi_municipalities mun ON onv.oktmo = mun.oktmo AND mun.deleted_at IS NULL
          WHERE req.state_service_id = 610 AND (req.status_code = ANY (ARRAY['to_review'::text, 'review'::text, 'accepted'::text, 'paper_waiting'::text, 'declined'::text, 'to_canceled'::text, 'canceled'::text])) AND req.report_year > 2019
          GROUP BY onv.service_request_id, onv.id, (COALESCE(onv.number, ''::text)), (
                CASE
                    WHEN onv.category IS NULL OR (onv.category = ANY (ARRAY[5, 0])) THEN 'Некатегорийный объект'::text
                    ELSE onv.category::text
                END), (COALESCE(onv.oktmo, ''::text)), onv.name, mun.name, (
                CASE
                    WHEN onv.onv_id IS NULL THEN 'Отсутствует в реестре НВОС'::text
                    ELSE 'Присутствует в реестре НВОС'::text
                END), cal.id, fkko.id, fkko.code, fkko.name, clas.name
        ),
        
        
        
         c30 AS (
         SELECT onv.service_request_id,
            onv.id AS onvos_id,
            onv.number AS onv_number,
            fkko.id AS fkko_id,
            fkko.code AS fkko_code,
            fkko.name AS fkko_name,
            sum(correct.events_sum) AS events_sum,
            sum(correct.sum_with_correction) AS sum_with_correction
           FROM dpnvos_r30_onvoses onv
             LEFT JOIN dpnvos_r30_documents doc ON doc.onvos_id = onv.id AND onv.deleted_at IS NULL AND doc.deleted_at IS NULL
             LEFT JOIN dpnvos_r30_corrections correct ON correct.document_id = doc.id AND correct.deleted_at IS NULL
             LEFT JOIN nsi_fkkos fkko ON correct.fkko_id = fkko.id
             LEFT JOIN nsi_waste_hazard_classes clas ON correct.waste_hazard_class_id = clas.id
             JOIN rpn_service_requests req ON onv.service_request_id = req.id AND req.deleted_at IS NULL
          WHERE req.state_service_id = 610 AND (req.status_code = ANY (ARRAY['to_review'::text, 'review'::text, 'accepted'::text, 'paper_waiting'::text, 'declined'::text, 'to_canceled'::text, 'canceled'::text])) AND req.report_year > 2019
          GROUP BY onv.service_request_id, onv.id, onv.number, fkko.id, fkko.code, fkko.name
        ), 
        
        
        report_final AS (
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
            report.app_okved,
            r30.service_request_id,
            r30.onvos_id,
            r30.onv_number,
            r30.onv_name,
            r30.onv_category,
            r30.is_in_reestr,
            r30.onvs_oktmo,
            r30.mun_name,
            r30.cal_id,
            r30.fkko_id,
            r30.fkko_code,
            r30.fkko_name,
            r30.class_name,
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
            COALESCE(onvs.okveds::text, report.app_okved) AS okved,
            LEAST(COALESCE(c30.events_sum, 0::double precision), r30.sum_all::double precision) AS sum_events,
            r30.sum_all::double precision - LEAST(COALESCE(c30.events_sum, 0::double precision), r30.sum_all::double precision) AS sum_budget
           FROM r30
             JOIN report ON report.req_id = r30.service_request_id
             LEFT JOIN c30 ON c30.onvos_id = r30.onvos_id AND r30.fkko_code = c30.fkko_code
             LEFT JOIN onvs ON r30.onv_number = onvs.number
        )
 SELECT row_number() OVER (ORDER BY report_final.report_year DESC, report_final.req_id, report_final.org_name, report_final.onv_number, report_final.onv_name) AS id,
    report_final.req_id,
    report_final.report_year,
    report_final.status,
    report_final.send_date,
    report_final.doc_vid,
    report_final.ter_name,
    report_final.region,
    report_final.mun_name,
    report_final.org_name AS name,
    report_final.inn,
    report_final.kpp,
    report_final.address,
    report_final.okved,
    report_final.onv_number,
    report_final.onv_name,
    report_final.onv_category,
    report_final.is_in_reestr,
    report_final.onvs_oktmo,
    report_final.fkko_code,
    report_final.fkko_name,
    report_final.class_name,
    report_final.formed_reporting_period,
    report_final.utilized_reporting_period,
    report_final.neutralized_reporting_period,
    report_final.actual_prev_period,
    report_final.actual_balance,
    report_final.transferred,
    report_final.place_in_period,
    report_final.including_underlimit,
    report_final.including_overlimit,
    report_final.sum_underlimit,
    report_final.sum_overlimit,
    report_final.sum_all,
    report_final.sum_events,
    report_final.sum_budget
   FROM report_final
  ORDER BY report_final.report_year DESC, report_final.req_id, report_final.org_name, report_final.onv_number, report_final.onv_name;