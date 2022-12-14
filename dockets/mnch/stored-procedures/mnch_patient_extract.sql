DELIMITER $$
CREATE  PROCEDURE `build_ndwr_mnch_patient_extract`(IN query_type varchar(50),IN queue_number int, IN queue_size int, IN cycle_size int,IN log BOOLEAN)
BEGIN

					set @primary_table := "ndwr_mnch_patients_extract";
                    set @total_rows_written = 0;
					set @start = now();
					set @table_version = "ndwr_mnch_patients_extract_v1.0";
                    set @query_type=query_type;
                    set @last_date_created := null;
                    set @last_date_created = (select max(DateCreated) from ndwr.ndwr_all_patients_extract);
                    set @endDate := LAST_DAY(CURDATE());

CREATE TABLE IF NOT EXISTS ndwr_mnch_patients_extract (
    `Pkv` VARCHAR(20) NULL,
    `PatientPK` INT NOT NULL,
    `SiteCode` INT NOT NULL,
    `PatientMNCH_ID` VARCHAR(30) NOT NULL,
    `EncounterID` INT NOT NULL,
    `VisitDate` DATETIME NOT NULL,
    `PatientHEI_ID` VARCHAR(30) NULL,
    `Emr` VARCHAR(50) NULL,
    `Project` VARCHAR(50) NULL,
    `FacilityName` VARCHAR(100) NULL,
    `Gender` VARCHAR(10) NULL,
    `DOB` DATETIME NOT NULL,
    `FirstEnrollmentAtMNCH` DATETIME NOT NULL,
    `Occupation` VARCHAR(100) NULL,
    `MaritalStatus` VARCHAR(100) NULL,
    `EducationLevel` VARCHAR(50) NULL,
    `PatientResidentCounty` VARCHAR(100) NULL,
    `PatientResidentSubCounty` VARCHAR(100) NULL,
    `PatientResidentWard` VARCHAR(100) NULL,
    `Inschool` VARCHAR(10) NULL,
    `DateCreated` TIMESTAMP DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
    INDEX patient_patient_pk (PatientPK),
    INDEX patient_site_code (SiteCode),
    INDEX patient_date_created (DateCreated),
    INDEX patient_patient_site_code (PatientPK , SiteCode)
);

                    if(@query_type="build") then

							              select 'BUILDING..........................................';
                            set @write_table = concat("ndwr_mnch_patient_extract_build_queue_temp_",queue_number);
                            set @queue_table = concat("ndwr_mnch_patient_extract_build_queue_",queue_number);

										  SET @dyn_sql=CONCAT('create table if not exists ',@write_table,' like ',@primary_table);
							              PREPARE s1 from @dyn_sql; 
							              EXECUTE s1; 
							              DEALLOCATE PREPARE s1;  


							              SET @dyn_sql=CONCAT('Create table if not exists ',@queue_table,' (select * from ndwr_mnch_patient_extract_build_queue limit ', queue_size, ');'); 
							              PREPARE s1 from @dyn_sql; 
							              EXECUTE s1; 
							              DEALLOCATE PREPARE s1;  

							              SET @dyn_sql=CONCAT('delete t1 from ndwr_mnch_patient_extract_build_queue t1 join ',@queue_table, ' t2 using (person_id);'); 
                                          PREPARE s1 from @dyn_sql; 
							              EXECUTE s1; 
							              DEALLOCATE PREPARE s1; 
                                          
										 
                                          
                                         
                                          
				  end if;

                  if (@query_type="sync") then
                            select 'SYNCING..........................................';
                           

                  end if;
                  
                  SET @person_ids_count = 0;
				  SET @dyn_sql=CONCAT('select count(*) into @person_ids_count from ',@queue_table); 
				  PREPARE s1 from @dyn_sql; 
				  EXECUTE s1; 
				  DEALLOCATE PREPARE s1;

SELECT @person_ids_count AS 'num patients to build';
                  
SELECT CONCAT('Deleting data from ', @primary_table);
                    
					SET @dyn_sql=CONCAT('delete t1 from ',@primary_table, ' t1 join ',@queue_table,' t2 on (t1.PatientPK = t2.person_id);'); 
                    PREPARE s1 from @dyn_sql; 
                    EXECUTE s1; 
                    DEALLOCATE PREPARE s1;

                    set @total_time=0;
                    set @cycle_number = 0;
                    set @last_encounter_date=null;
                    set @status=null;                            
					set @last_encounter_date=null;                            
					set @rtc_date=null; 

                    while @person_ids_count > 0 do

                        	set @loop_start_time = now();
							drop  table if exists ndwr_mnch_patient_extract_build_queue__0;

                                      SET @dyn_sql=CONCAT('create temporary table if not exists ndwr_mnch_patient_extract_build_queue__0 (person_id int primary key) (select * from ',@queue_table,' limit 1);'); 
						              PREPARE s1 from @dyn_sql; 
						              EXECUTE s1; 
						              DEALLOCATE PREPARE s1;

SELECT CONCAT('Creating soundex mapping ...');

                          drop temporary table if exists ndwr_patient_pkv_occcupation_mapping;


                          create temporary TABLE  ndwr_patient_pkv_occcupation_mapping(
                              SELECT 
                                    q.person_id,
                                    p.gender,
                                    p.birthdate,
                                    h.identifier as PatientHEI_ID,
                                    CASE
                                     WHEN u.identifier IS NOT NULL THEN  u.identifier
                                     ELSE an.identifier
                                    END AS 'PatientMNCH_ID',
                                    cn.name as 'Occupation',
                                    en.name as 'EducationLevel',
                                    addr.address1 as 'PatientResidentCounty',
                                    addr.address2 as 'PatientResidentSubCounty',
                                    CONCAT(p.gender,n.given_name,SOUNDEX(n.given_name),SOUNDEX(n.family_name),DATE_FORMAT(p.birthdate,'%Y')) as 'Pkv'
                                FROM
                                    ndwr.ndwr_mnch_patient_extract_build_queue__0 q
                                    left join amrs.person p on (p.person_id = q.person_id AND p.voided = 0)
                                    left join amrs.person_name n on (n.person_id = q.person_id AND n.voided = 0)
                                    left join amrs.person_attribute a on (a.person_id = q.person_id AND a.person_attribute_type_id = 42 AND a.voided = 0)
                                    left join amrs.person_attribute e on (e.person_id = q.person_id AND e.person_attribute_type_id = 73 AND e.voided = 0)
                                    left join amrs.concept_name cn on (cn.concept_id = a.value and cn.locale_preferred = 1 AND a.value != 5622)
                                    left join amrs.concept_name en on (en.concept_id = e.value and en.locale_preferred = 1 AND e.value != 5622)
                                    left join amrs.patient_identifier h on (h.patient_id = q.person_id AND h.identifier_type = 38 AND h.voided = 0)
                                    left join amrs.patient_identifier u on (u.patient_id = q.person_id AND u.identifier_type = 8 AND u.voided = 0)
									left join amrs.patient_identifier an on (an.patient_id = q.person_id AND an.identifier_type = 3 AND an.voided = 0)
                                    left join amrs.person_address addr on (addr.person_id = q.person_id and addr.voided = 0)
                                    group by q.person_id

                          );
                          
						-- Get patients latest pmtct or hei clinical encounter
                        
                        SELECT CONCAT('getting latest enocunter ...');
                        
                        drop temporary table if exists ndwr_mnch_patient_extract_latest_encounter;
                        create temporary table ndwr_mnch_patient_extract_latest_encounter(
                        SELECT 
                                m.*,
								fa.encounter_datetime as VisitDate,
                                fa.encounter_id as EncounterID,
                                fa.location_id
							FROM
								ndwr_patient_pkv_occcupation_mapping m
									JOIN
								etl.flat_appointment fa ON (fa.person_id = m.person_id)
							WHERE
									fa.program_id IN (4 , 29)
                                    AND fa.is_clinical = 1
									order by fa.encounter_datetime desc
									limit 1
                        
                        );
                        
                         SELECT CONCAT('getting FirstEnrollmentAtMNCH ...');
                        
                         drop temporary table if exists ndwr_mnch_patient_extract_mnch_first_enrollment;
                        create temporary table ndwr_mnch_patient_extract_mnch_first_enrollment(
                        SELECT 
                                m.*,
								fa.encounter_datetime as 'FirstEnrollmentAtMNCH'
							FROM
								ndwr_mnch_patient_extract_latest_encounter m
									JOIN
								etl.flat_appointment fa ON (fa.person_id = m.person_id)
							WHERE
									fa.program_id IN (4 , 29)
                                    AND fa.is_clinical = 1
									order by fa.encounter_datetime asc
									limit 1
                        
                        );
                        
                        

						SELECT CONCAT('Creating ndwr_mnch_patient_extract_interim table ...');
                                      
						  
                          drop temporary table if exists ndwr_mnch_patient_extract_interim;
                          create temporary table ndwr_mnch_patient_extract_interim (
                          SELECT
                           Pkv,
                           l.person_id AS PatientPK,
                           mfl.mfl_code as SiteCode,
                           PatientMNCH_ID,
                           EncounterID,
                           VisitDate,
                           PatientHEI_ID,
						  'AMRS' AS Emr,
						  'Ampath Plus' AS 'Project',
						   mfl.Facility AS FacilityName,
						   l.gender as Gender,
						   l.birthdate as DOB,
						   FirstEnrollmentAtMNCH,
						   l.Occupation AS 'Occupation',
						   NULL AS MaritalStatus,
						   EducationLevel,
						   PatientResidentCounty,
						   PatientResidentSubCounty,
						   NULL AS PatientResidentWard,
						   NULL AS Inschool,
                           NULL AS DateCreated
                            FROM 
                            ndwr_mnch_patient_extract_mnch_first_enrollment l
                             JOIN
                          ndwr.mfl_codes mfl ON (mfl.location_id = l.location_id)
                          );
                          


                        
                          

SELECT 
    COUNT(*)
INTO @new_encounter_rows FROM
    ndwr_mnch_patient_extract_interim;
SELECT @new_encounter_rows;                    
                          set @total_rows_written = @total_rows_written + @new_encounter_rows;
SELECT @total_rows_written;

                          SET @dyn_sql=CONCAT('replace into ',@write_table,'(select * from ndwr_mnch_patient_extract_interim)');

                          PREPARE s1 from @dyn_sql; 
                          EXECUTE s1; 
                          DEALLOCATE PREPARE s1;

                          SET @dyn_sql=CONCAT('delete t1 from ',@queue_table,' t1 join ndwr_mnch_patient_extract_build_queue__0 t2 using (person_id);'); 
						  PREPARE s1 from @dyn_sql; 
                          EXECUTE s1; 
						  DEALLOCATE PREPARE s1;  
                        

						 SET @dyn_sql=CONCAT('select count(*) into @person_ids_count from ',@queue_table,';'); 
                         PREPARE s1 from @dyn_sql; 
                         EXECUTE s1; 
                         DEALLOCATE PREPARE s1;
                         
                         set @cycle_length = timestampdiff(second,@loop_start_time,now());
                         set @total_time = @total_time + @cycle_length;
                         set @cycle_number = @cycle_number + 1;
                         set @remaining_time = ceil((@total_time / @cycle_number) * ceil(@person_ids_count / cycle_size) / 60);
                         
SELECT 
    @person_ids_count AS 'persons remaining',
    @cycle_length AS 'Cycle time (s)',
    CEIL(@person_ids_count / cycle_size) AS remaining_cycles,
    @remaining_time AS 'Est time remaining (min)';


                    end while;

                         SET @dyn_sql=CONCAT('drop table ',@queue_table,';');
                         PREPARE s1 from @dyn_sql;
                         EXECUTE s1;
                         DEALLOCATE PREPARE s1;  

                         SET @total_rows_to_write=0;
                         SET @dyn_sql=CONCAT("Select count(*) into @total_rows_to_write from ",@write_table);
                         PREPARE s1 from @dyn_sql; 
                         EXECUTE s1; 
                         DEALLOCATE PREPARE s1;

                         set @start_write = now();
                         
SELECT 
    CONCAT(@start_write,
            ' : Writing ',
            @total_rows_to_write,
            ' to ',
            @primary_table);

                        SET @dyn_sql=CONCAT('replace into ', @primary_table,'(select * from ',@write_table,');');
                        PREPARE s1 from @dyn_sql; 
                        EXECUTE s1;
                        DEALLOCATE PREPARE s1;

SELECT 
    CONCAT(@finish_write,
            ' : Completed writing rows. Time to write to primary table: ',
            @time_to_write,
            ' seconds ');
            
                        SET @dyn_sql=CONCAT('drop table ',@write_table,';');
                        PREPARE s1 from @dyn_sql; 
                        EXECUTE s1; 
                        DEALLOCATE PREPARE s1;
                        
                        
                        set @ave_cycle_length = ceil(@total_time/@cycle_number);
SELECT 
    CONCAT('Average Cycle Length: ',
            @ave_cycle_length,
            'second(s)');
                        set @end = now();
                        
insert into ndwr.flat_log values (@start,@last_date_created,@table_version,timestampdiff(second,@start,@end));
                        
SELECT 
    CONCAT(@table_version,
            ' : Time to complete: ',
            TIMESTAMPDIFF(MINUTE, @start, @end),
            ' minutes');


END$$
DELIMITER ;
