DELIMITER $$
CREATE DEFINER=`fmaiko`@`%` PROCEDURE `createPatientNDWRDataSets_test`(
  IN selectedMFLCode int(11),
  IN selectedFacility varchar(250),
  IN selectedPatient int(11)
  
  )
BEGIN
                 DECLARE selectedPeriod date; 
                 DECLARE selectedMFLCode INT Default 0;
SELECT 
    reporting_period
FROM
    ndwr.mfl_period
LIMIT 1 INTO selectedPeriod;
 		SELECT 
    mfl_code
FROM
    ndwr.mfl_period
LIMIT 1 INTO selectedMFLCode;
 
                          
 						  set @selectedPatient:=selectedPatient;
                           Set @facilityName:= selectedFacility;
                           set @siteCode:= @selectedMFLCode; 
                           set @selectedPeriod:= selectedPeriod;
                           set @selectedMFLCode:= selectedMFLCode;
DELETE FROM ndwr_base_line_0;
   							insert into ndwr_base_line_0						
   							SELECT 
   							distinct
   							s.person_id,
   							l.location_id as location_id,
   							l.test_datetime AS test_datetime,
   							if(s.arv_first_regimen_start_date,s.arv_first_regimen_start_date,s.enrollment_date) as arv_first_regimen_start_date,
                              s.cur_who_stage,
   							DATE_ADD(if(s.arv_first_regimen_start_date,s.arv_first_regimen_start_date,s.enrollment_date),
   								INTERVAL 12 MONTH) AS after_12,
   							DATE_ADD(if(s.arv_first_regimen_start_date,s.arv_first_regimen_start_date,s.enrollment_date),
   								INTERVAL 6 MONTH) AS after_6,
   							DATEDIFF(l.test_datetime,
   									if(s.arv_first_regimen_start_date,s.arv_first_regimen_start_date,s.enrollment_date)) AS close_first_regimen_start,
   							SIGN(DATEDIFF(l.test_datetime,
   											if(s.arv_first_regimen_start_date,s.arv_first_regimen_start_date,s.enrollment_date))) lab_art_start_comparison,
   							ABS(DATEDIFF(l.test_datetime,
   											DATE_ADD(if(s.arv_first_regimen_start_date,s.arv_first_regimen_start_date,s.enrollment_date),
   												INTERVAL 6 MONTH))) AS test_close_to_6,
   							ABS(DATEDIFF(l.test_datetime,
   											DATE_ADD(if(s.arv_first_regimen_start_date,s.arv_first_regimen_start_date,s.enrollment_date),
   												INTERVAL 12 MONTH))) AS test_close_to_12,
   							IF(l.obs REGEXP '!!5497=[0-9]',
   								CAST(REPLACE(REPLACE((SUBSTRING_INDEX(SUBSTRING(l.obs,
   														LOCATE('!!5497=', l.obs)),
   													'##',
   													1)),
   											'!!5497=',
   											''),
   										'!!',
   										'')
   									AS UNSIGNED),
   								NULL) AS cd4,
   							IF(l.obs REGEXP '!!730=[0-9]',
   								CAST(REPLACE(REPLACE((SUBSTRING_INDEX(SUBSTRING(l.obs,
   														LOCATE('!!730=', l.obs)),
   													'##',
   													1)),
   											'!!730=',
   											''),
   										'!!',
   										'')
   									AS UNSIGNED),
   								NULL) AS cd4_percent,
   							IF(l.obs REGEXP '!!856=[0-9]',
   								CAST(REPLACE(REPLACE((SUBSTRING_INDEX(SUBSTRING(l.obs,
   														LOCATE('!!856=', l.obs)),
   													'##',
   													1)),
   											'!!856=',
   											''),
   										'!!',
   										'')
   									AS UNSIGNED),
   								NULL) AS v_l
   							
   
                               FROM
                                   etl.flat_hiv_summary_v15b s
                                   LEFT JOIN
                                   etl.flat_lab_obs l USING (person_id)                                
                                   WHERE s.person_id =@selectedPatient
  								 and l.person_id =@selectedPatient
  								 and s.location_id in (
  								 select location_id from ndwr.mfl_codes where mfl_code=@selectedMFLCode)
  								 and s.person_id =@selectedPatient								 
  								 and (s.arv_first_regimen_start_date <> ''	or s.enrollment_date<>'')			
   							;
     
                          call buildNDWRPatientBaseline(@selectedMFLCode,selectedFacility,@selectedPatient);
  						
DELETE FROM ndwr.ndwr_visit_0;
  						
   						insert into ndwr.ndwr_visit_0 							
   							 SELECT
   										person_id,
   										cur_arv_adherence,
   										edd,
   										if(edd,'Yes',null) as pregnant,
   										if(edd,date_add(edd, interval -280 day),null) as LMP,
   										if(edd,datediff(encounter_datetime,date_add(edd, interval -280 day)),null) as gestation,
   										null as condoms_provided ,
   										contraceptive_method AS pwp,
   										IF(contraceptive_method,
   										contraceptive_method,
   										null) AS family_planning,
   										cur_who_stage,
   										encounter_datetime,
   										location_id,
   										scheduled_visit,
   										encounter_id,
   										rtc_date
   									FROM etl.flat_hiv_summary_v15b
  									WHERE person_id =@selectedPatient
  								    and location_id in (
  								         select location_id from ndwr.mfl_codes where mfl_code=@selectedMFLCode)
  								 
   							;						
   							
   							DELETE FROM ndwr.ndwroi;
   							insert into ndwr.ndwroi
   								select t1.person_id,
   								if(obs regexp "!!6042=" ,
   										replace(replace((substring_index(substring(obs,locate("!!6042=",obs)),'##',ROUND ((LENGTH(obs) - LENGTH( REPLACE ( obs, "!!6042=", "") ) ) / LENGTH("!!6042=") ))),"!!6042=",""),"!!",""),
   										null
   								) as OI, t1.encounter_datetime as OIDate 
   								
   								 from etl.flat_obs t1 
                                   inner join ndwr.ndwr_visit_0 t2 on t2.encounter_datetime=t1.encounter_datetime and t1.person_id=t2.person_id
   								where t1.person_id =@selectedPatient
  								and t1.location_id in (select location_id from ndwr.mfl_codes 
  								where mfl_code=@selectedMFLCode)
  								and obs regexp "!!6042="
   
   							;
                                                          
   							
   							#Find lab_art_start_comparison					
   							
  								
  							
                      
             
			replace into ndwr.ndwr_patient_status_extract(
                select
                   t1.PatientPK,
                   t1.PatientID,
                   t1.FacilityId,
                   t1.SiteCode,
				   t1.Emr,
                   t1.Project,
                   t1.FacilityName,
  				   if(t1.StatusAtCCC in('dead','ltfu','transfer_out'),StatusAtCCC,null) as ExitDescription,
				   if(t1.StatusAtCCC in('dead','ltfu','transfer_out'),t1.lastVisit,null) as ExitDate,
                   if(t1.StatusAtCCC in('dead','ltfu','transfer_out'),StatusAtCCC,null) as ExitReason
                 
                from ndwr.ndwr_all_patients t1 where t1.StatusAtCCC in('dead','ltfu','transfer_out')
  			  and t1.PatientID=@selectedPatient 
               );
   
               # ART ndwr_patient_status
  			 set @cur_arv_meds=null;
  			 set @cur_arv_line_strict=null;
  			 insert into ndwr.ndwr_patient_art_extract(           
               select  distinct
               t1.PatientPK,
               t1.PatientID,
               t1.FacilityID,
               t1.SiteCode,
               t1.Emr,
			   t1.Project,
               t1.FacilityName,
               t1.DOB as DOB,
			   DATEDIFF(t1.RegistrationDate,DOB)/365.25 as AgeEnrollment,
               if(sign(DATEDIFF(t1.arv_first_regimen_start_date,DOB)/365.25)=-1,DATEDIFF(t1.RegistrationDate,DOB)/365.25, DATEDIFF(t1.arv_first_regimen_start_date,t1.DOB)/365.25) as AgeARTStart,
			   DATEDIFF(t1.lastVisit,t1.DOB)/365.25 as AgeLastVisit,
               RegistrationDate,
               null as PatientSource,
               t1.gender as Gender,
               case
                  when RegistrationDate <= if(DATE(t1.arv_first_regimen_start_date) = '1900-01-01','1997-01-01',t1.arv_first_regimen_start_date) then RegistrationDate
                  when RegistrationDate > if(DATE(t1.arv_first_regimen_start_date) = '1900-01-01','1997-01-01',t1.arv_first_regimen_start_date) then DATE_ADD(RegistrationDate, INTERVAL 30 DAY)
               end as `StartARTDate`,
                case
                  when RegistrationDate <= if(DATE(t1.arv_first_regimen_start_date) = '1900-01-01','1997-01-01',t1.arv_first_regimen_start_date) then RegistrationDate
                  when RegistrationDate > if(DATE(t1.arv_first_regimen_start_date) = '1900-01-01','1997-01-01',t1.arv_first_regimen_start_date) then DATE_ADD(RegistrationDate, INTERVAL 30 DAY)
               end as `PreviousARTStartDate`,
               etl.get_arv_names(t1.arv_first_regimen) as PreviousARTRegimen,
               t1.arv_start_date as StartARTAtThisFacility,
			   t1.arv_first_regimen as StartRegimen,
               case
   								when  @cur_arv_line_strict is null 
  								then @cur_arv_line_strict := t1.cur_arv_line_strict
   								else @cur_arv_line_strict 
   			 end as StartRegimenLine,
             t1.lastVisit as LastARTDate,
             case
					when  @cur_arv_meds is null then @cur_arv_meds := t1.cur_arv_meds
					else @cur_arv_meds 
   			 end as LastRegimen,
             case
					when  @cur_arv_line_strict is null then @cur_arv_line_strict := t1.cur_arv_line_strict
					else @cur_arv_line_strict
   			 end as LastRegimenLine,
             DATEDIFF(t1.rtc_date,t1.lastVisit) as Duration,
			 t1.rtc_date as ExpectedReturn,
             'Government' as Provider,
             t1.LastVisit,
			 if(t1.StatusAtCCC in('dead','ltfu','transfer_out'),StatusAtCCC,null) as ExitReason,
			 if(t1.StatusAtCCC in('dead','ltfu','transfer_out'),t1.lastVisit,null) as ExitDate
               
               FROM ndwr.ndwr_all_patients t1 where patientid=@selectedPatient);
  			 
             replace into ndwr.ndwr_patient_labs_extract
                   (SELECT 
                       t.person_id AS PatientPK,
                       t.person_id AS PatientID,
                       @siteCode AS FacilityID,
                       @siteCode AS SiteCode,
					   'AMRS' AS Emr,
					   'Ampath Plus' AS Project,
                       @facilityName AS FacilityName,
                       null AS SatelliteName,
                       t.VisitID,
                       t.test_datetime as OrderedbyDate,
                       t.test_datetime as ReportedbyDate,
                       t.TestName,
					   null AS EnrollmentTest,
					   t.TestResult,
                       t.TestName as Reason
                      
                           
                   FROM
                       (SELECT 
                           t1.person_id,
                               t1.test_datetime,
                               'CD4 Count' AS TestName,
                               CAST(REPLACE(REPLACE((SUBSTRING_INDEX(SUBSTRING(t1.obs,
                                                                           LOCATE('!!5497=', t1.obs)),
                                                                       '##',
                                                                       1)),
                                                               '!!5497=',
                                                               ''),
                                                           '!!',
                                                           '')
                                                       AS UNSIGNED) AS TestResult,
                               encounter_id as VisitID
   
                       FROM
                       etl.flat_lab_obs t1       
  					 WHERE 	t1.person_id = @selectedPatient					 
  					 and t1.obs REGEXP '!!5497=[0-9]' 
                           
                           
                           UNION  
                           
                           SELECT 
                           t2.person_id,
                               t2.test_datetime,
                               'CD4 %' AS TestName,
                               CAST(REPLACE(REPLACE((SUBSTRING_INDEX(SUBSTRING(t2.obs,
                                                                           LOCATE('!!730=', t2.obs)),
                                                                       '##',
                                                                       1)),
                                                               '!!730=',
                                                               ''),
                                                           '!!',
                                                           '')
                                                       AS UNSIGNED)   AS TestResult,
                               t2.encounter_id as VisitID
   
                       FROM
                           etl.flat_lab_obs t2   WHERE t2.person_id = @selectedPatient and t2.obs REGEXP '!!730=[0-9]'
                           
                           Union 
                           SELECT 
                           t3.person_id,
                           t3.test_datetime,
                               'VL' AS TestName,
                               CAST(REPLACE(REPLACE((SUBSTRING_INDEX(SUBSTRING(t3.obs,
                                                                           LOCATE('!!856=', t3.obs)),
                                                                       '##',
                                                                       1)),
                                                               '!!856=',
                                                               ''),
                                                           '!!',
                                                           '')
                                                       AS UNSIGNED) AS TestResult,
                               t3.encounter_id as VisitID
   
                       FROM
                           etl.flat_lab_obs  t3  WHERE   t3.person_id = @selectedPatient and t3.obs REGEXP '!!856=[0-9]'
                           
                           ) t);  
                           
				replace into ndwr.ndwr_patient_adverse_events (
                 select
                   t1.PatientPK,
                   t1.PatientID,
                   t1.FacilityId,
                   t1.SiteCode,
				   t1.Emr as EMR,
                   t1.Project,
                   NULL AS AdverseEvent,
				   NULL AS AdverseEventStartDate,
				   NULL AS AdverseEventEndDate,
				   NULL AS Severity,
				   NULL AS VisitDate,
				   NULL AS AdverseEventActionTaken,
				   NULL AS AdverseEventClinicalOutcome,
				   NULL AS AdverseEventIsPregnant,
				   NULL AS AdverseEventCause,
				   NULL AS AdverseEventRegimen
                    
                 FROM ndwr.ndwr_all_patients t1 where patientid=@selectedPatient
                
                );
                                         
END$$
DELIMITER ;