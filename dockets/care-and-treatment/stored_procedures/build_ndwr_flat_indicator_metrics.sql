CREATE PROCEDURE `ndwr`.`build_ndwr_flat_indicator_metrics`(IN previousMonthEndDate DATE, IN thisMonthEndDate DATE)
BEGIN

    IF NOT EXISTS (
    SELECT 1 FROM information_schema.tables WHERE table_schema = DATABASE() AND table_name = 'ndwr_flat_indicator_metrics') THEN
        CREATE TABLE ndwr_flat_indicator_metrics (
            -- Define the columns of the table here
            MFL_CODE smallint PRIMARY KEY,
            HTS_TESTED smallint default 0,
            HTS_TESTED_POS smallint default 0,
            HTS_LINKED smallint default 0,
            HTS_INDEX smallint default 0,
            HTS_INDEX_POS smallint default 0,
            TX_NEW smallint default 0,
            TX_CURR smallint default 0,
            TX_RTT smallint default 0,
            TX_ML smallint default 0,
    		TX_PVLS decimal(10,4) default 0,
            MMD smallint default 0,
            RETENTION_ON_ART_12_MONTHS smallint default 0,
            RETENTION_ON_ART_VL_1000_12_MONTHS smallint default 0,
            LAST_ENCOUNTER_CREATE_DATE DATE default null,
            EMR_ETL_Refresh DATE default null         
        );
    END IF;
      
    -- Check if the table is null then add MFL_CODEs
    IF (SELECT COUNT(*) FROM ndwr_flat_indicator_metrics) = 0 THEN
        -- save all mfl-codes set for DWAPI
        INSERT INTO ndwr_flat_indicator_metrics (MFL_CODE)
        SELECT b.MFL_CODE FROM (
          SELECT mfl_code AS `MFL_CODE` FROM ndwr.mfl_codes group by mfl_code
        ) b;
    END IF;

    -- 1. Insert TX-ML
    UPDATE ndwr_flat_indicator_metrics m
    JOIN (
        SELECT mc.mfl_code AS `MFL_CODE`, COUNT(hm.person_id) AS `TX_ML`
        FROM etl.hiv_monthly_report_dataset_frozen hm
        INNER JOIN ndwr.mfl_codes mc ON (hm.location_id = mc.location_id)
        LEFT JOIN etl.hiv_monthly_report_dataset_frozen tx ON (hm.person_id = tx.person_id AND tx.on_art_this_month = 1)
        WHERE hm.endDate = thisMonthEndDate AND hm.on_art_this_month = 0 AND tx.endDate = previousMonthEndDate
        GROUP BY mc.mfl_code
    ) b ON m.MFL_CODE = b.MFL_CODE
    SET m.TX_ML = b.TX_ML;
    
    -- 2. Insert TX_CURR
    UPDATE ndwr_flat_indicator_metrics m
    JOIN (
        SELECT mc.mfl_code AS `MFL_CODE`, COUNT(hm.person_id) AS `TX_CURR`
        FROM etl.hiv_monthly_report_dataset_frozen hm
        INNER JOIN ndwr.mfl_codes mc ON (hm.location_id = mc.location_id)
        WHERE hm.endDate = thisMonthEndDate AND hm.on_art_this_month = 1 AND hm.status = 'active'
        GROUP BY mc.mfl_code
    ) b ON m.MFL_CODE = b.MFL_CODE
    SET m.TX_CURR = b.TX_CURR;
   
    -- 3. Insert TX_NEW
    UPDATE ndwr_flat_indicator_metrics m
    JOIN (
        SELECT mc.mfl_code AS `MFL_CODE`, COUNT(hm.person_id) AS `TX_NEW`
        FROM etl.hiv_monthly_report_dataset_frozen hm
        INNER JOIN ndwr.mfl_codes mc ON (hm.location_id = mc.location_id)
        WHERE hm.endDate = thisMonthEndDate AND hm.started_art_this_month = 1
        GROUP BY mc.mfl_code
    ) b ON m.MFL_CODE = b.MFL_CODE
    SET m.TX_NEW = b.TX_NEW;
   
    -- 4. Insert TX_RTT
    UPDATE ndwr_flat_indicator_metrics m
    JOIN (
        SELECT mc.mfl_code AS `MFL_CODE`, COUNT(hm.person_id) AS `TX_RTT`
        FROM etl.hiv_monthly_report_dataset_frozen hm
        INNER JOIN ndwr.mfl_codes mc ON (hm.location_id = mc.location_id)
        LEFT JOIN etl.hiv_monthly_report_dataset_frozen tx ON (hm.person_id = tx.person_id AND tx.on_art_this_month = 0)
        WHERE hm.endDate = thisMonthEndDate AND hm.on_art_this_month = 1 AND tx.endDate = previousMonthEndDate
        GROUP BY mc.mfl_code
    ) b ON m.MFL_CODE = b.MFL_CODE
    SET m.TX_RTT = b.TX_RTT;
   
    -- 5. Insert RETENTION_ON_ART_12_MONTHS
    UPDATE ndwr_flat_indicator_metrics m
    JOIN (
        SELECT mc.mfl_code AS `MFL_CODE`, COUNT(hm.person_id) AS `RETENTION_ON_ART_12_MONTHS`
        FROM etl.hiv_monthly_report_dataset_frozen hm
        INNER JOIN ndwr.mfl_codes mc ON (hm.location_id = mc.location_id)
        WHERE DATEDIFF(thisMonthEndDate, enrollment_date) <= 365 AND endDate = thisMonthEndDate AND on_art_this_month = 1
        GROUP BY mc.mfl_code
    ) b ON m.MFL_CODE = b.MFL_CODE
    SET m.RETENTION_ON_ART_12_MONTHS = b.RETENTION_ON_ART_12_MONTHS;
   
  -- 6. Insert LAST_ENCOUNTER_CREATE_DATE
    UPDATE ndwr_flat_indicator_metrics m
    SET m.LAST_ENCOUNTER_CREATE_DATE = thisMonthEndDate;
   
    -- 7. Insert EMR_ETL_Refresh
    UPDATE ndwr_flat_indicator_metrics m
    SET m.EMR_ETL_Refresh = thisMonthEndDate;
   
    -- 8. Insert RETENTION_ON_ART_VL_1000_12_MONTHS
    UPDATE ndwr_flat_indicator_metrics m
    JOIN (
        SELECT mc.mfl_code AS `MFL_CODE`, COUNT(hm.person_id) AS `RETENTION_ON_ART_VL_1000_12_MONTHS`
        FROM etl.hiv_monthly_report_dataset_frozen hm
        INNER JOIN ndwr.mfl_codes mc ON (hm.location_id = mc.location_id)
        WHERE hm.endDate = thisMonthEndDate AND hm.vl_suppressed_12_month_cohort = 1
        GROUP BY mc.mfl_code
    ) b ON m.MFL_CODE = b.MFL_CODE
    SET m.RETENTION_ON_ART_VL_1000_12_MONTHS = b.RETENTION_ON_ART_VL_1000_12_MONTHS;
   
    -- 9. Insert TX_PVLS
    UPDATE ndwr_flat_indicator_metrics m
    JOIN (
	    SELECT mc.mfl_code AS `MFL_CODE`, ROUND((COUNT(hm.person_id) / total_count.total) * 100, 4) AS `TX_PVLS`
		FROM
		    etl.hiv_monthly_report_dataset_frozen hm
		    INNER JOIN ndwr.mfl_codes mc ON (hm.location_id = mc.location_id)
		    INNER JOIN (
		        SELECT mcj.mfl_code, COUNT(cj.person_id) AS total
		        FROM etl.hiv_monthly_report_dataset_frozen cj
		        INNER JOIN ndwr.mfl_codes mcj ON (cj.location_id = mcj.location_id)
		        WHERE cj.endDate = thisMonthEndDate
		        GROUP BY mcj.mfl_code
		    ) AS total_count ON mc.mfl_code = total_count.mfl_code
		WHERE
		    hm.endDate = thisMonthEndDate AND hm.vl_suppressed_12_month_cohort = 1
		GROUP BY
		    mc.mfl_code
    ) b ON m.MFL_CODE = b.MFL_CODE
    SET m.TX_PVLS = b.TX_PVLS;
   
    -- 10. Insert MMD
    UPDATE ndwr_flat_indicator_metrics m
    JOIN (
        SELECT mc.mfl_code AS `MFL_CODE`, COUNT(hm.person_id) AS `MMD`
        FROM etl.hiv_monthly_report_dataset_frozen hm
        INNER JOIN ndwr.mfl_codes mc ON (hm.location_id = mc.location_id)
        WHERE hm.endDate = thisMonthEndDate AND hm.days_since_rtc_date <= -90
        GROUP BY mc.mfl_code
    ) b ON m.MFL_CODE = b.MFL_CODE
    SET m.MMD = b.MMD;
   
END