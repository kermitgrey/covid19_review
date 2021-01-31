CREATE OR REPLACE PACKAGE covid19
AS
PROCEDURE load_regions;

PROCEDURE load_divisions;

PROCEDURE load_states;

PROCEDURE load_counties;

PROCEDURE load_censusregions;

PROCEDURE load_cases_deaths;

PROCEDURE load_population;

PROCEDURE load_cdc_deaths;

PROCEDURE update_average_deaths;

PROCEDURE run_data_load;

v_err_num	number;                                                                                  
v_err_msg       varchar2(250 char); 

END covid19;
/
CREATE OR REPLACE PACKAGE BODY covid19
AS

PROCEDURE load_regions
IS
/***************************************************************
* Build  table containing the census bureau regions using data in 
* the ext_census_state_geo_codes_load external table.
****************************************************************/

BEGIN
    
    EXECUTE IMMEDIATE 'ALTER TABLE cov_regionsdivisions DISABLE CONSTRAINT cov_region_FK';
    EXECUTE IMMEDIATE 'TRUNCATE TABLE cov_regions';
   
      
    INSERT INTO cov_regions( regionid,
                                                regionname)
    SELECT DISTINCT region, regionname
     FROM ext_census_state_geo_codes_load
     ORDER BY region;

    COMMIT;
    
    EXECUTE IMMEDIATE 'ALTER TABLE cov_regionsdivisions ENABLE CONSTRAINT cov_region_FK';

EXCEPTION
    WHEN OTHERS THEN
      v_err_num := sqlcode;
      v_err_msg := substr(sqlerrm(v_err_num), 1, 250);
      
      INSERT INTO error_log(progname,
                              progtime,
                              errcode,
                              errmesg)
      VALUES('load_regions',
                sysdate,
                v_err_num,
                v_err_msg);
                
END load_regions;

PROCEDURE load_divisions
IS
/***************************************************************
* Build  table containing the census bureau divisions using data in 
* the ext_census_state_geo_codes_load external table.
****************************************************************/

BEGIN
    
    EXECUTE IMMEDIATE 'ALTER TABLE cov_regionsdivisions DISABLE CONSTRAINT cov_division_FK';
    EXECUTE IMMEDIATE 'TRUNCATE TABLE cov_divisions';
    
         
    INSERT INTO cov_divisions(divisionid,
                                                divisionname)
    SELECT DISTINCT division, divisionname
    FROM ext_census_state_geo_codes_load
    ORDER BY division;
                
    COMMIT;
    
    EXECUTE IMMEDIATE 'ALTER TABLE cov_regionsdivisions ENABLE CONSTRAINT cov_division_FK';
    
EXCEPTION
    WHEN OTHERS THEN
      v_err_num := sqlcode;
      v_err_msg := substr(sqlerrm(v_err_num), 1, 250);
      
      INSERT INTO error_log(progname,
                              progtime,
                              errcode,
                              errmesg)
      VALUES('load_divisions',
                sysdate,
                v_err_num,
                v_err_msg);
                
END load_divisions;

PROCEDURE load_states
IS
/***************************************************************
* Build table containing the statefips-statecode-statename from 
* data in the ext_census_state_geo_codes_load external table.
****************************************************************/

BEGIN
    
    EXECUTE IMMEDIATE 'ALTER TABLE cov_fipscounty DISABLE CONSTRAINT cov_fipscounty_stateid_FK';
    EXECUTE IMMEDIATE 'TRUNCATE TABLE cov_fipsstate';
    

      
    INSERT INTO cov_fipsstate(statefips,
                                                    statecode,
                                                    statename)
    SELECT DISTINCT statefips, statecode, statename
    FROM ext_census_state_geo_codes_load
    ORDER BY statefips;
                
    COMMIT;
    
    EXECUTE IMMEDIATE 'ALTER TABLE cov_fipscounty ENABLE CONSTRAINT cov_fipscounty_stateid_FK';
    
EXCEPTION
    WHEN OTHERS THEN
      v_err_num := sqlcode;
      v_err_msg := substr(sqlerrm(v_err_num), 1, 250);
      
      INSERT INTO error_log(progname,
                              progtime,
                              errcode,
                              errmesg)
      VALUES('load_states',
                sysdate,
                v_err_num,
                v_err_msg);
                
END load_states;

PROCEDURE load_counties
IS
/***************************************************************
* Build  table containing the countyfips-countyname using data in 
* the ext_census_county_geo_codes_load external table.
****************************************************************/

BEGIN
    
    EXECUTE IMMEDIATE 'ALTER TABLE cov_census DISABLE CONSTRAINT cov_census_cty_FK';
    EXECUTE IMMEDIATE 'ALTER TABLE cov_cases DISABLE CONSTRAINT cov_cases_FK';
    EXECUTE IMMEDIATE 'ALTER TABLE cov_deaths DISABLE CONSTRAINT cov_deaths_FK';
    EXECUTE IMMEDIATE 'ALTER TABLE cov_cdc_deaths DISABLE CONSTRAINT cov_cdc_deaths_FK';
    EXECUTE IMMEDIATE 'TRUNCATE TABLE cov_fipscounty';
          
    INSERT INTO cov_fipscounty(statefips,
                                                    countyfips,
                                                    countyname)
    SELECT statefips, 
                    countyfips, 
                    countyname
    FROM ext_census_county_geo_codes_load
    ORDER BY statefips, countyfips;
                
    COMMIT;
    
    EXECUTE IMMEDIATE 'ALTER TABLE cov_census ENABLE CONSTRAINT cov_census_cty_FK';
    EXECUTE IMMEDIATE 'ALTER TABLE cov_cases ENABLE CONSTRAINT cov_cases_FK';
    EXECUTE IMMEDIATE 'ALTER TABLE cov_deaths ENABLE CONSTRAINT cov_deaths_FK';
    EXECUTE IMMEDIATE 'ALTER TABLE cov_cdc_deaths ENABLE CONSTRAINT cov_cdc_deaths_FK';
    
EXCEPTION
    WHEN OTHERS THEN
      v_err_num := sqlcode;
      v_err_msg := substr(sqlerrm(v_err_num), 1, 250);
      
      INSERT INTO error_log(progname,
                              progtime,
                              errcode,
                              errmesg)
      VALUES('load_counties',
                sysdate,
                v_err_num,
                v_err_msg);
                
END load_counties;

PROCEDURE load_censusregions
IS
/**************************************************************************
* Build a lookup table that links the census regionid/divisionid 
* to a statefips code.  Use the data in the ext_census_state_geo_codes_load 
* external table for the build.
***************************************************************************/

BEGIN
    
    EXECUTE IMMEDIATE 'TRUNCATE TABLE cov_regionsdivisions';
      
    INSERT INTO cov_regionsdivisions( regionid,
                                                                divisionid,
                                                                statefips)
    SELECT DISTINCT region, division, statefips
    FROM ext_census_state_geo_codes_load;
        
    COMMIT;
    
EXCEPTION
    WHEN OTHERS THEN
      v_err_num := sqlcode;
      v_err_msg := substr(sqlerrm(v_err_num), 1, 250);
      
      INSERT INTO error_log(progname,
                              progtime,
                              errcode,
                              errmesg)
      VALUES('load_censusregions',
                sysdate,
                v_err_num,
                v_err_msg);
                
END load_censusregions;

PROCEDURE load_cases_deaths
IS
/**********************************************************************************
* Build covid19 cases & deaths table from the external table ext_covid_data_load
* Load only those countyfips recs that exist in cov_fipscounty.
**********************************************************************************/
BEGIN
    
    EXECUTE IMMEDIATE 'TRUNCATE TABLE cov_cases';
    EXECUTE IMMEDIATE 'TRUNCATE TABLE cov_deaths';
    
    INSERT INTO cov_cases(statefips,
                                           countyfips,
                                           reportdate,
                                           quantity,
                                           loaddate)
    SELECT a.statefips, 
                    a.countyfips, 
                    a.reportdate, 
                    a.quantity, 
                    sysdate
    FROM ext_covid_data_load a
    WHERE a.type = 'c'
    AND EXISTS (SELECT 1
                            FROM cov_fipscounty b
                            WHERE a.statefips = b.statefips
                            AND a.countyfips = b.countyfips);
            
    COMMIT;
    
    INSERT INTO cov_deaths(statefips,
                                           countyfips,
                                           reportdate,
                                           quantity,
                                           loaddate)
    SELECT a.statefips, 
                    a.countyfips, 
                    a.reportdate, 
                    a.quantity, 
                    sysdate
    FROM ext_covid_data_load a
    WHERE a.type = 'd'
    AND EXISTS (SELECT 1
                            FROM cov_fipscounty b
                            WHERE a.statefips = b.statefips
                            AND a.countyfips = b.countyfips);
            
    COMMIT;  
    
EXCEPTION
    WHEN OTHERS THEN
      v_err_num := sqlcode;
      v_err_msg := substr(sqlerrm(v_err_num), 1, 250);
      
      INSERT INTO error_log(progname,
                              progtime,
                              errcode,
                              errmesg)
      VALUES('load_cases_deaths',
                sysdate,
                v_err_num,
                v_err_msg);
                
END load_cases_deaths;

PROCEDURE load_population
IS
/***************************************************************
* Build census population estimates table using data in the 
* external table ext_census_pop_estimate_load.
* Load only those countyfips recs that exist in cov_fipscounty.
****************************************************************/
BEGIN
    
    EXECUTE IMMEDIATE 'TRUNCATE TABLE cov_census';
 
    INSERT INTO cov_census(statefips,
                                                        countyfips,
                                                        pop2010,
                                                        pop2019
                                                        )
    SELECT a.statefips, 
                    a.countyfips, 
                    a.pop2010,
                    a.pop2019
    FROM ext_census_pop_estimate_load a
    WHERE  EXISTS (SELECT 1
                                FROM cov_fipscounty b
                                WHERE a.statefips = b.statefips
                                AND a.countyfips = b.countyfips);
            
    COMMIT;  
    
EXCEPTION
    WHEN OTHERS THEN
      v_err_num := sqlcode;
      v_err_msg := substr(sqlerrm(v_err_num), 1, 250);
      
      INSERT INTO error_log(progname,
                              progtime,
                              errcode,
                              errmesg)
      VALUES('load_population',
                sysdate,
                v_err_num,
                v_err_msg);
                
END load_population;

PROCEDURE load_cdc_deaths
IS
/***********************************************************************
* Build table containing annual deaths information for multiple years
* by state and county from the external table ext_cdc_deaths_load.
* Load only those statefips recs that exist in cov_fipscounty.  Also,
* populate the ethnic codes table using data from the same external table.
*************************************************************************/
BEGIN

-- fill cdc deaths ethnic codes from external table

    EXECUTE IMMEDIATE 'ALTER TABLE cov_cdc_deaths DISABLE CONSTRAINT cov_cdc_deaths_codes_FK';
    EXECUTE IMMEDIATE 'TRUNCATE TABLE cov_cdc_deaths_codes';
    
    INSERT INTO cov_cdc_deaths_codes(ethniccode,
                                                                     ethniclabel)
    SELECT DISTINCT
            CASE WHEN hispanic_origin_code = '2135-2' THEN '2135-2'
            ELSE race_code END,
            CASE WHEN hispanic_origin_code = '2135-2' THEN 'Hispanic'
            ELSE race END
    FROM ext_cdc_deaths_load;
    
    COMMIT;
    
    EXECUTE IMMEDIATE 'ALTER TABLE cov_cdc_deaths ENABLE CONSTRAINT cov_cdc_deaths_codes_FK';
    
-- population cdc deaths table from external    
    EXECUTE IMMEDIATE 'TRUNCATE TABLE cov_cdc_deaths';
      
    INSERT INTO cov_cdc_deaths(statefips,
                                                            countyfips,
                                                            gender,
                                                            ethniccode,
                                                            reportyear,
                                                            deaths)
    SELECT a.state_code,
                a.county_code,
                a.gender_code,
                CASE WHEN a.hispanic_origin_code = '2135-2' THEN '2135-2'
                ELSE a.race_code END,
                a.reportyear,
                sum(a.deaths)
    FROM ext_cdc_deaths_load a
    WHERE EXISTS (SELECT 1
                               FROM cov_fipscounty b
                               WHERE a.state_code = b.statefips
                               AND a.county_code = b.countyfips)
    GROUP BY a.state_code, 
                        a.county_code, 
                        a.gender_code,
                        CASE WHEN a.hispanic_origin_code = '2135-2' THEN '2135-2'
                        ELSE a.race_code END,
                        a.reportyear;

    COMMIT;
    
EXCEPTION
    WHEN OTHERS THEN
      v_err_num := sqlcode;
      v_err_msg := substr(sqlerrm(v_err_num), 1, 250);
      
      INSERT INTO error_log(progname,
                              progtime,
                              errcode,
                              errmesg)
      VALUES('load_cdc_deaths',
                sysdate,
                v_err_num,
                v_err_msg);
                
END load_cdc_deaths;

PROCEDURE update_average_deaths
IS
/***************************************************************
* Update the annual_avg_all_deaths column in cov_census from data
* in the cov_cdc_deaths table.
****************************************************************/
BEGIN
        
    UPDATE cov_census a
    SET a.annual_avg_all_deaths =  (SELECT  sum(deaths)/4
                                                            FROM cov_cdc_deaths b
                                                            WHERE a.statefips = b.statefips
                                                            AND a.countyfips = b.countyfips);
    COMMIT;
    
EXCEPTION
    WHEN OTHERS THEN
      v_err_num := sqlcode;
      v_err_msg := substr(sqlerrm(v_err_num), 1, 250);
      
      INSERT INTO error_log(progname,
                              progtime,
                              errcode,
                              errmesg)
      VALUES('update_average_deaths',
                sysdate,
                v_err_num,
                v_err_msg);
                
END update_average_deaths;

PROCEDURE run_data_load
IS
/***************************************************************
* execute all procedures in this package.
* 
****************************************************************/
BEGIN

    load_regions;
    
    load_divisions;
    
    load_states;
    
    load_counties;
    
    load_censusregions;
    
    load_cases_deaths;
    
    load_population;
    
    load_cdc_deaths;
    
    update_average_deaths;
    
EXCEPTION
    WHEN OTHERS THEN
      v_err_num := sqlcode;
      v_err_msg := substr(sqlerrm(v_err_num), 1, 250);
      
      INSERT INTO error_log(progname,
                              progtime,
                              errcode,
                              errmesg)
      VALUES('execute_all_procedures',
                sysdate,
                v_err_num,
                v_err_msg);
                
END run_data_load;

END covid19;
