use ndwr;
CREATE TABLE ndwr_patient_eac_build_queue (
    person_id INT(6) UNSIGNED,
    INDEX person_id_eac (person_id)
);