CREATE OR REPLACE PACKAGE         "TERRA_DOTTA_EXTRACT" AS 

  /* TODO enter package declarations (types, exceptions, methods etc) here */ 
  PROCEDURE TD_execute;
  
  PROCEDURE TD_Build_Extract_Table;
  
  PROCEDURE TD_Write_File;

END TERRA_DOTTA_EXTRACT;
/


CREATE OR REPLACE PACKAGE BODY                                     "TERRA_DOTTA_EXTRACT" AS

/**************************************************************************************************************************************************
   NAME:       Terra Dotta
   PURPOSE:

   REVISIONS:
   Ver        Date        Author           Description
   ---------  ----------  ---------------  ------------------------------------
    1.1       11/06/2015  astegner         Removed line to not restrict nbrjobs_suff to '00' - line marked with 11/06/2015
    1.2       04/15/2016  astegner         No longer excluding Temp employees (TB, TF, TM) per Randy Ulrey.
    1.3       07/17/2020  dshussain        Updated nbrjobs_status = 'A" to in ('A','B') per HR
    1.4       10/26/2022  astegner         per T.Shafer fix suggestion, added CASE to handle 'FC' state codes in spraddr_stat_code section of perm/MA address
    1.5       10/22/2025  cgin             add hr_code and college to extract
 **************************************************************************************************************************************************/   

  PROCEDURE TD_execute AS
    lv_Procedure        VARCHAR2(200) := 'B_FIN.TERRA_DOTTA_EXTRACT.TD_execute  ';

  BEGIN
    -- TODO: Implementation required for PROCEDURE TERRA_DOTTA_EXTRACT.TD_execute
  TERRA_DOTTA_EXTRACT.TD_Build_Extract_Table;
  TERRA_DOTTA_EXTRACT. TD_Write_File;

    COMMIT;
    
  EXCEPTION
         WHEN OTHERS THEN
             DBMS_OUTPUT.PUT_LINE(lv_procedure || substr(SQLERRM,1,400) || substr(DBMS_UTILITY.FORMAT_ERROR_BACKTRACE,1,400));
             RAISE;  
    
  END TD_execute;

  PROCEDURE TD_Build_Extract_Table AS
  lv_location                     varchar2(30);
  lv_procedure                    VARCHAR2(200) := 'B_FIN.TERRA_DOTTA_EXTRACT.TD_Build_Extract_Table  ';
  BEGIN
    -- clean out rows from last run
    execute immediate ('truncate table b_fin.terra_dotta');                     -- 2022-10-26 line added
--    delete from b_fin.terra_dotta;                                            -- 2022-10-26 line removed
    commit;

lv_location := 'initial insert';
-- insert all employees and GA's into table
insert into b_fin.terra_dotta
(PERS_NET_ID, LAST_NAME, GIVEN_NAME, MIDDLE_NAME, TITLE, DEPARTMENT, STUDENT_ID, COLLEGE, HR_CODE)
select distinct
gobumap_udc_id PERS_NET_ID,
spriden_last_name LAST_NAME,
spriden_first_name GIVEN_NAME, 
nvl(spriden_mi,' ') MIDDLE_NAME, 
nbrjobs_desc TITLE, 
ftvorgn_title DEPARTMENT,
spriden.spriden_id STUDENT_ID,
nvl(st.SGBSTDN_COLL_CODE_1,'') COLLEGE,
case
    when emp.spriden_pidm is not null
         then 'Y'
    else
         'N'
end HR_CODE
from nbrjobs 
left outer join nbrbjob on nbrjobs.nbrjobs_pidm = nbrbjob.nbrbjob_pidm and nbrjobs.nbrjobs_posn = nbrbjob.nbrbjob_posn and nbrjobs.nbrjobs_suff = nbrbjob.nbrbjob_suff 
left outer join spriden on nbrjobs.nbrjobs_pidm = spriden.spriden_pidm and spriden_change_ind is null 
left outer join ftvorgn on nbrjobs.nbrjobs_orgn_code_ts = ftvorgn.ftvorgn_orgn_code 
left outer JOIN B_VIEW.IDENTIFIER_XWALK IDX ON spriden.SPRIDEN_PIDM = IDX.SPRIDEN_PIDM
left outer join 
          (SELECT *
          FROM 
            (SELECT SGBSTDN_PIDM
                  ,SGBSTDN_LEVL_CODE
                  ,SGBSTDN_TERM_CODE_EFF
                  ,SGBSTDN_COLL_CODE_1
                  ,SGBSTDN_DEPT_CODE
                  ,SGBSTDN_MAJR_CODE_1
                  ,SGBSTDN_MAJR_CODE_MINR_1
                  ,ROW_NUMBER() OVER (PARTITION BY SGBSTDN_PIDM ORDER BY NVL(SGBSTDN_TERM_CODE_EFF,'190010') DESC) RANK
            FROM SGBSTDN 
            inner join sfrstcr on sgbstdn_pidm = sfrstcr_pidm
            WHERE sfrstcr_term_code >= B_GEN.FN_GET_CURRENT_TERM
            AND sfrstcr_rsts_code in ('AU','FL','RE','RW')
            )
          WHERE RANK = 1          
          ) ST 
       ON ST.SGBSTDN_PIDM = nbrjobs.nbrjobs_PIDM 
left outer join       
        (select s1.spriden_pidm
        from spriden s1
        inner join pebempl empl on s1.spriden_pidm = empl.pebempl_pidm and empl.PEBEMPL_EMPL_STATUS = 'A'
        left outer join sibinst inst on s1.spriden_pidm = inst.sibinst_pidm and inst.SIBINST_FCST_CODE = 'AC' and inst.SIBINST_SCHD_IND = 'Y'
        where s1.spriden_change_ind is null
        ) emp on nbrjobs.nbrjobs_PIDM = emp.spriden_pidm       
where nbrjobs.nbrjobs_status in('A' ,'B')                                                   -- 2020/07/17 was nbrjobs_status = 'A', updated per HR request DSH
--and nbrjobs.nbrjobs_suff = '00'                                                           --11/06/2015 line removed
and nbrjobs.nbrjobs_effective_date = (select max(nbrjobs_effective_date) from nbrjobs x 
                                      where x.nbrjobs_pidm = nbrjobs.nbrjobs_pidm 
                                      and x.nbrjobs_posn = nbrjobs.nbrjobs_posn 
                                      and x.nbrjobs_suff = nbrjobs.nbrjobs_suff 
                                      and x.nbrjobs_effective_date <= sysdate) 
and nbrbjob.nbrbjob_contract_type in 'P'
and (nbrbjob.nbrbjob_end_date is null or nbrbjob.nbrbjob_end_date > sysdate) 
and ftvorgn.ftvorgn_eff_date <= sysdate 
and ftvorgn.ftvorgn_nchg_date > sysdate 
--and nbrjobs.nbrjobs_ecls_code NOT IN('LR','S1','S2','TB','TF','TM') -- LR(retirees) - S1,S2(non-GA students) - TB,TF,TM(temps)  --04/15/2016 line removed
and nbrjobs.nbrjobs_ecls_code NOT IN('LR','S1','S2')                  -- 04/15/2016 no longer excluding TB,TF,TM(temps)
;
-- insert all retirees who have an active secondary contract who aren't already in the table
insert into b_fin.terra_dotta
(PERS_NET_ID, LAST_NAME, GIVEN_NAME, MIDDLE_NAME, TITLE, DEPARTMENT, STUDENT_ID, COLLEGE, HR_CODE)
select distinct
gobumap_udc_id PERS_NET_ID,
spriden_last_name LAST_NAME,
spriden_first_name GIVEN_NAME, 
nvl(spriden_mi,' ') MIDDLE_NAME, 
nbrjobs_desc TITLE, 
ftvorgn_title DEPARTMENT,
spriden.spriden_id STUDENT_ID,
nvl(st.SGBSTDN_COLL_CODE_1,'') COLLEGE,
case
    when emp.spriden_pidm is not null
         then 'Y'
    else
         'N'
end HR_CODE
from nbrjobs 
left outer join nbrbjob on nbrjobs.nbrjobs_pidm = nbrbjob.nbrbjob_pidm and nbrjobs.nbrjobs_posn = nbrbjob.nbrbjob_posn and nbrjobs.nbrjobs_suff = nbrbjob.nbrbjob_suff 
left outer join spriden on nbrjobs.nbrjobs_pidm = spriden.spriden_pidm and spriden_change_ind is null 
left outer join ftvorgn on nbrjobs.nbrjobs_orgn_code_ts = ftvorgn.ftvorgn_orgn_code 
left outer JOIN B_VIEW.IDENTIFIER_XWALK IDX ON spriden.SPRIDEN_PIDM = IDX.SPRIDEN_PIDM
left outer join 
          (SELECT *
          FROM 
            (SELECT SGBSTDN_PIDM
                  ,SGBSTDN_LEVL_CODE
                  ,SGBSTDN_TERM_CODE_EFF
                  ,SGBSTDN_COLL_CODE_1
                  ,SGBSTDN_DEPT_CODE
                  ,SGBSTDN_MAJR_CODE_1
                  ,SGBSTDN_MAJR_CODE_MINR_1
                  ,ROW_NUMBER() OVER (PARTITION BY SGBSTDN_PIDM ORDER BY NVL(SGBSTDN_TERM_CODE_EFF,'190010') DESC) RANK
            FROM SGBSTDN 
            inner join sfrstcr on sgbstdn_pidm = sfrstcr_pidm
            WHERE sfrstcr_term_code >= B_GEN.FN_GET_CURRENT_TERM
            AND sfrstcr_rsts_code in ('AU','FL','RE','RW')
            )
          WHERE RANK = 1          
          ) ST 
       ON ST.SGBSTDN_PIDM = nbrjobs.nbrjobs_PIDM 
left outer join       
        (select s1.spriden_pidm
        from spriden s1
        inner join pebempl empl on s1.spriden_pidm = empl.pebempl_pidm and empl.PEBEMPL_EMPL_STATUS = 'A'
        left outer join sibinst inst on s1.spriden_pidm = inst.sibinst_pidm and inst.SIBINST_FCST_CODE = 'AC' and inst.SIBINST_SCHD_IND = 'Y'
        where s1.spriden_change_ind is null
        ) emp on nbrjobs.nbrjobs_PIDM = emp.spriden_pidm    
left outer join b_fin.terra_dotta on pers_net_id = gobumap_udc_id
where nbrjobs.nbrjobs_status in('A' ,'B')                                                   -- 2020/07/17 was nbrjobs_status = 'A', updated per HR request DSH
--and nbrjobs.nbrjobs_suff = '00'                                                           --11/06/2015 line removed
and nbrjobs.nbrjobs_effective_date = (select max(nbrjobs_effective_date) from nbrjobs x 
                                      where x.nbrjobs_pidm = nbrjobs.nbrjobs_pidm 
                                      and x.nbrjobs_posn = nbrjobs.nbrjobs_posn 
                                      and x.nbrjobs_suff = nbrjobs.nbrjobs_suff 
                                      and x.nbrjobs_effective_date <= sysdate) 
and nbrbjob.nbrbjob_contract_type in 'S'
and pers_net_id is null
and (nbrbjob.nbrbjob_end_date is null or nbrbjob.nbrbjob_end_date > sysdate) 
and ftvorgn.ftvorgn_eff_date <= sysdate 
and ftvorgn.ftvorgn_nchg_date > sysdate 
--and nbrjobs.nbrjobs_ecls_code NOT IN('LR','S1','S2','TB','TF','TM') -- LR(retirees) - S1,S2(non-GA students) - TB,TF,TM(temps)  --04/15/2016 line removed
and nbrjobs.nbrjobs_ecls_code NOT IN('LR','S1','S2')                  -- 04/15/2016 no longer excluding TB,TF,TM(temps)
;
commit;

lv_location := 'add email address';
-- primary email address, if exists
update b_fin.terra_dotta 
 set (
 PRIMARY_EMAIL) = 
 (select distinct
    goremal_email_address
  from (select
          spriden.spriden_id,
          goremal_email_address
        from spriden
        inner join b_fin.terra_dotta td on spriden.spriden_id = td.student_id        
        left outer join goremal on spriden_pidm = goremal_pidm
        where goremal_preferred_ind = 'Y' 
        and spriden_change_ind is null
        and goremal_pidm is not null
      ) emal       
  where emal.spriden_id = b_fin.terra_dotta.student_id
 )
;
commit;

lv_location := 'add emergency contact';
-- emergency contact, if exists
update b_fin.terra_dotta 
 set (
  EMER_ADDR_NAME,
  EMER_ADDR_PHONE) = 
 (select
    spremrg_last_name || ', ' || spremrg_first_name,
    spremrg_phone_area || spremrg_phone_number
  from (select distinct
          spriden.spriden_id,
          spremrg_last_name,
          spremrg_first_name,
          spremrg_phone_area,
          spremrg_phone_number
        from spriden
        inner join b_fin.terra_dotta td on spriden.spriden_id = td.student_id        
        left outer join spremrg on spriden_pidm = spremrg_pidm
        where spremrg_priority = (select max(emrg2.spremrg_priority)
                                  from spremrg emrg2
                                  where emrg2.spremrg_pidm = spriden_pidm)
        and spriden_change_ind is null
        and spremrg_pidm is not null
      ) emrg       
  where emrg.spriden_id = b_fin.terra_dotta.student_id
 )
;
commit;

lv_location := 'add telephone number';
-- telephone number, if exists
update b_fin.terra_dotta 
 set (
 PERM_ADDR_PHONE) = 
 (select
    tele.sprtele_phone_area || tele.sprtele_phone_number
  from (select distinct
          spriden.spriden_id,
          sprtele_phone_area,
          sprtele_phone_number
        from spriden
        inner join b_fin.terra_dotta td on spriden.spriden_id = td.student_id        
        left outer join sprtele on spriden_pidm = sprtele_pidm
        where sprtele_primary_ind = 'Y' 
        and spriden_change_ind is null
        and sprtele_atyp_code = 'MA'
        and sprtele_addr_seqno = (select max(tele2.sprtele_addr_seqno)
                                  from sprtele tele2
                                  where tele2.sprtele_pidm = spriden_pidm
                                  and tele2.sprtele_atyp_code = 'MA')
        and sprtele_pidm is not null
      ) tele       
  where tele.spriden_id = b_fin.terra_dotta.student_id
 )
;
commit;

lv_location := 'add perm address';
-- add permanent address, if on file
update b_fin.terra_dotta
set (PERM_ADDR_LINE1,
 PERM_ADDR_LINE2,
 PERM_ADDR_CITY,
 PERM_ADDR_STATE,
 PERM_ADDR_POSTAL) = 
 (select
    spraddr_street_line1,
    spraddr_street_line2,
    spraddr_city,
    spraddr_stat_code,
    spraddr_zip
  FROM (select distinct
        spriden_id,
        spraddr_street_line1,
        spraddr_street_line2,
        spraddr_city,
--        spraddr_stat_code,                                                                              --2022-10-26 line removed
        CASE WHEN LENGTH(SPRADDR_STAT_CODE) > 2  AND (NVL(SPRADDR_NATN_CODE,'US') <> 'US') THEN 'FC'      --2022-10-26 CASE added
            ELSE SPRADDR_STAT_CODE
        END as spraddr_stat_code,
        spraddr_zip
      from spriden
      inner join b_fin.terra_dotta td on spriden.spriden_id = td.student_id
      left outer join spraddr on spriden_pidm = spraddr_pidm
      where spraddr_atyp_code = 'MA' 
      and spraddr_status_ind is null
      and spriden_change_ind is null
      and spraddr_seqno = (select max(addr2.spraddr_seqno)
                          from spraddr addr2
                          where addr2.spraddr_pidm = spriden_pidm
                          and addr2.spraddr_atyp_code = 'MA')
      and spraddr_pidm is not null
      ) ma_addr
  where ma_addr.spriden_id = b_fin.terra_dotta.student_id
 )
;
commit;

lv_location := 'add work address';
-- employee work address, if exists
update b_fin.terra_dotta 
 set (
 BILL_ADDR_LINE1,
 BILL_ADDR_CITY,
 BILL_ADDR_STATE,
 BILL_ADDR_POSTAL) = 
 (select
    spraddr_street_line1,
    spraddr_city,
    spraddr_stat_code,
    spraddr_zip
  from (select distinct
          spriden.spriden_id,
          spraddr_street_line1,  
          spraddr_city,
          spraddr_stat_code,
          spraddr_zip
        from spriden
        inner join b_fin.terra_dotta td on spriden.spriden_id = td.student_id        
        left outer join spraddr on spriden_pidm = spraddr_pidm
        where spraddr_atyp_code = 'EO' 
        and spraddr_status_ind is null
      and spraddr_seqno = (select max(addr2.spraddr_seqno)
                          from spraddr addr2
                          where addr2.spraddr_pidm = spriden_pidm
                          and addr2.spraddr_atyp_code = 'EO')
        and spriden_change_ind is null

        and spraddr_pidm is not null
      ) eo_addr       
  where eo_addr.spriden_id = b_fin.terra_dotta.student_id
 )
;
commit;

  EXCEPTION
    WHEN OTHERS THEN
     ROLLBACK;
     DBMS_OUTPUT.PUT_LINE('**ERROR** An unspecified error occurred during processing at ' || lv_location || '.');
     DBMS_OUTPUT.PUT_LINE(lv_procedure || substr(SQLERRM,1,400) || substr(DBMS_UTILITY.FORMAT_ERROR_BACKTRACE,1,400));
     RAISE;  
  
  END TD_Build_Extract_Table;

  PROCEDURE TD_Write_File AS
      lv_term                         VARCHAR2(6 CHAR) := '201430';  -- First term in Banner
      lv_start_date_time              date := SYSDATE;
      lv_td_environment               VARCHAR2(5)   := '';
      lv_oracle_instance              VARCHAR2(4)   := '';
      lv_file_directory               varchar2(200);
      lv_out_file_name                varchar2(200);
      lv_out_file                     UTL_FILE.file_type;
      lv_out_header_row               varchar2(1000) := '';
      lv_procedure                    VARCHAR2(200) := 'B_FIN.TERRA_DOTTA_EXTRACT.TD_Write_File  ';

  CURSOR cursor_terra_dotta IS
  select distinct 
 PERS_NET_ID || chr(9) ||
 LAST_NAME || chr(9) ||
 GIVEN_NAME || chr(9) ||
 MIDDLE_NAME || chr(9) ||
 PRIMARY_EMAIL || chr(9) ||
 TITLE || chr(9) ||
 DEPARTMENT || chr(9) ||
 STUDENT_ID || chr(9) ||
 PERM_ADDR_LINE1 || chr(9) ||
 PERM_ADDR_LINE2 || chr(9) ||
 PERM_ADDR_CITY || chr(9) ||
 PERM_ADDR_STATE || chr(9) ||
 PERM_ADDR_POSTAL || chr(9) ||
 PERM_ADDR_PHONE || chr(9) ||
 BILL_ADDR_LINE1 || chr(9) ||
 BILL_ADDR_CITY || chr(9) ||
 BILL_ADDR_STATE || chr(9) ||
 BILL_ADDR_POSTAL || chr(9) ||
 EMER_ADDR_NAME || chr(9) ||
 EMER_ADDR_PHONE || chr(9) ||
 COLLEGE || chr(9) ||
 HR_CODE as td_row
  from b_fin.terra_dotta
  ;
  
BEGIN

  lv_file_directory := 'UPLOAD_FINANCE';
  lv_out_file_name := 'tr_sis_hr_user_info.txt';
  lv_out_file := UTL_FILE.FOPEN(lv_file_directory, lv_out_file_name, 'W', 32767);

  lv_out_header_row := 'PERS_NET_ID' || chr(9);
  lv_out_header_row := lv_out_header_row || 'LAST_NAME' || chr(9);
  lv_out_header_row := lv_out_header_row || 'GIVEN_NAME' || chr(9);
  lv_out_header_row := lv_out_header_row || 'MIDDLE_NAME' || chr(9);
  lv_out_header_row := lv_out_header_row || 'PRIMARY_EMAIL' || chr(9);
  lv_out_header_row := lv_out_header_row || 'TITLE' || chr(9);
  lv_out_header_row := lv_out_header_row || 'DEPARTMENT' || chr(9);
  lv_out_header_row := lv_out_header_row || 'STUDENT_ID' || chr(9);
  lv_out_header_row := lv_out_header_row || 'PERM_ADDR_LINE1' || chr(9);
  lv_out_header_row := lv_out_header_row || 'PERM_ADDR_LINE2' || chr(9);
  lv_out_header_row := lv_out_header_row || 'PERM_ADDR_CITY' || chr(9);
  lv_out_header_row := lv_out_header_row || 'PERM_ADDR_STATE' || chr(9);
  lv_out_header_row := lv_out_header_row || 'PERM_ADDR_POSTAL' || chr(9);
  lv_out_header_row := lv_out_header_row || 'PERM_ADDR_PHONE' || chr(9);
  lv_out_header_row := lv_out_header_row || 'BILL_ADDR_LINE1' || chr(9);
  lv_out_header_row := lv_out_header_row || 'BILL_ADDR_CITY' || chr(9);
  lv_out_header_row := lv_out_header_row || 'BILL_ADDR_STATE' || chr(9);
  lv_out_header_row := lv_out_header_row || 'BILL_ADDR_POSTAL' || chr(9);
  lv_out_header_row := lv_out_header_row || 'EMER_ADDR_NAME' || chr(9);
  lv_out_header_row := lv_out_header_row || 'EMER_ADDR_PHONE' || chr(9);
  lv_out_header_row := lv_out_header_row || 'COLLEGE' || chr(9);
  lv_out_header_row := lv_out_header_row || 'HR_CODE' || chr(13);
  
  UTL_FILE.PUT_LINE(lv_out_file, lv_out_header_row, TRUE);
   
  FOR cursor_terra_dotta_row in cursor_terra_dotta
  LOOP
    UTL_FILE.PUT_LINE(lv_out_file, cursor_terra_dotta_row.td_row || chr(13), TRUE);
  END LOOP;

  UTL_FILE.FCLOSE(lv_out_file);
   
  COMMIT;
  
  EXCEPTION
    WHEN utl_file.invalid_path THEN
      raise_application_error(-20000, '**ERROR** Invalid PATH for file: ' || nvl(lv_file_directory,'no path name') || nvl(lv_out_file_name,'no file name'));
    WHEN OTHERS THEN
      DBMS_OUTPUT.PUT_LINE(lv_procedure || substr(SQLERRM,1,400) || substr(DBMS_UTILITY.FORMAT_ERROR_BACKTRACE,1,400));
      RAISE;  
      
END TD_Write_File;

END TERRA_DOTTA_EXTRACT;
/
