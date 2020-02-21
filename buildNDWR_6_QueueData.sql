CREATE  PROCEDURE `buildNDWR_6_QueueData`(IN selectedMFLCode INT)
BEGIN
  DECLARE n INT DEFAULT 0;
         DECLARE i INT DEFAULT 0;
 		 DECLARE selectedPatient INT Default 0;
 		 DECLARE selectedFacility varchar(200) Default 0;
         SELECT  @beforeCal:=NOW();
 		 Select facility from ndwr.mfl_codes where mfl_code = selectedMFLCode limit 1 into selectedFacility;
         delete from ndwr_baseline_queue_6 where person_id in(select patientid from ndwr.ndwr_base_line);
		 SELECT count(*) from ndwr_baseline_queue_6  INTO n;
	                     	
         SET i=0;
 		

         WHILE i<n DO  		
				SELECT distinct person_id  FROM ndwr_baseline_queue_6 limit i,1 into selectedPatient;
				
				call createPatientNDWRDataSets(selectedMFLCode,selectedFacility,selectedPatient);	
 				insert into progress(status,queue) select  concat(((i/n) * 60),' % of ', n) as '% complete', 6 as queue;
				           	   
           SET i = i + 1;
  end while; 
  delete from ndwr_baseline_queue_6;             
  SELECT NOW() as currentTime,@beforeCal as StartTime;
 END