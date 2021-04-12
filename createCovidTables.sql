DROP TABLE cov_regionsdivisions;
DROP TABLE cov_cases;
DROP TABLE cov_deaths;
DROP TABLE cov_census;
DROP TABLE cov_fipscounty;
DROP TABLE cov_fipsstate;
DROP TABLE cov_regions;
DROP TABLE cov_divisions;


CREATE TABLE cov_fipsstate(
statefips             	varchar2(2 char) NOT NULL,
statecode           	varchar2(2 char) NOT NULL,
statename		varchar2(50 char) NOT NULL,
constraint cov_fipsstate_PK Primary Key(statefips)
);

CREATE TABLE cov_fipscounty(
statefips               VARCHAR2(2 CHAR) NOT NULL,
countyfips              VARCHAR2(5 CHAR) NOT NULL,
countyname          	VARCHAR2(100 CHAR) NOT NULL,
constraint cov_fipscounty_PK Primary Key(statefips, countyfips),
constraint cov_fipscounty_stateid_FK Foreign Key(statefips) references cov_fipsstate
);

CREATE TABLE cov_regions(
regionid          	NUMBER NOT NULL,
regionname      	VARCHAR2(50 CHAR) NOT NULL,
constraint cov_regions_PK Primary Key(regionid)
);

CREATE TABLE cov_divisions(
divisionid              NUMBER NOT NULL,
divisionname            VARCHAR2(50 CHAR) NOT NULL,
constraint cov_divisions_PK Primary Key(divisionid)
);

CREATE TABLE cov_regionsdivisions(
regionid               	NUMBER NOT NULL,
divisionid              NUMBER NOT NULL,
statefips               VARCHAR2(2 CHAR) NOT NULL,
constraint cov_regdiv_PK Primary Key (regionid, divisionid, statefips),
constraint cov_region_FK Foreign Key(regionid) references cov_regions,
constraint cov_division_FK Foreign Key(divisionid) references cov_divisions
);

CREATE TABLE cov_census(
statefips		VARCHAR2(2 CHAR) NOT NULL,
countyfips		VARCHAR2(5 CHAR) NOT NULL,
pop2010			NUMBER NOT NULL,
pop2019			NUMBER NOT NULL,
annual_avg_all_deaths	NUMBER,
constraint cov_census_PK Primary Key(statefips, countyfips),
constraint cov_census_cty_FK Foreign Key(statefips, countyfips) references cov_fipscounty
);

CREATE TABLE cov_cases(
statefips		VARCHAR2(2 CHAR) NOT NULL,
countyfips		VARCHAR2(5 CHAR) NOT NULL,
reportdate		DATE NOT NULL,
quantity		NUMBER NOT NULL,
loaddate		DATE,
constraint cov_cases_PK Primary Key(statefips, countyfips, reportdate),
constraint cov_cases_FK Foreign Key(statefips, countyfips) references cov_fipscounty
);

CREATE TABLE cov_deaths(
statefips		VARCHAR2(2 CHAR) NOT NULL,
countyfips		VARCHAR2(5 CHAR) NOT NULL,
reportdate		DATE NOT NULL,
quantity		NUMBER NOT NULL,
loaddate		DATE,
constraint cov_deaths_PK Primary Key(statefips, countyfips, reportdate),
constraint cov_deaths_FK Foreign Key(statefips, countyfips) references cov_fipscounty
);

-- create external tables to facilitate query of external data
-- previously written to files via python.

DROP TABLE ext_census_state_geo_codes_load;
DROP TABLE ext_census_county_geo_codes_load;
DROP TABLE ext_census_pop_estimate_load;
DROP TABLE ext_covid_data_load;

CREATE TABLE ext_census_state_geo_codes_load(
region		number,
regionname	varchar2(30 char),
division	number,
divisionname	varchar2(30 char),
statefips	varchar2(2 char),
statename	varchar2(30 char),
statecode	varchar2(2 char)
)
    ORGANIZATION EXTERNAL
    (
        TYPE ORACLE_LOADER
        DEFAULT DIRECTORY ext_data_dir
        ACCESS PARAMETERS
        (
            records delimited by '\n'
            readsize 2777834
            skip 1
            badfile ext_bad_dir:'stategeos.bad'
            logfile ext_log_dir: 'stategeos.log'
            fields terminated by ',' optionally enclosed by '"' NOTRIM
            missing field values are null
            reject rows with all null fields
            (
		region,
		regionname,
		division,
		divisionname,
		statefips,
		statename,
		statecode
	    )
	)
	LOCATION('censusregdivstateout.csv')
)
 REJECT LIMIT UNLIMITED;

CREATE TABLE ext_census_county_geo_codes_load(
statefips               VARCHAR2(2 CHAR),
countyfips              VARCHAR2(5 CHAR),
countyname          	VARCHAR2(100 CHAR)
)
    ORGANIZATION EXTERNAL
    (
        TYPE ORACLE_LOADER
        DEFAULT DIRECTORY ext_data_dir
        ACCESS PARAMETERS
        (
            records delimited by '\n'
            readsize 2777834
            skip 1
            badfile ext_bad_dir:'countygeos.bad'
            logfile ext_log_dir: 'countygeos.log'
            fields terminated by ',' optionally enclosed by '"' NOTRIM
            missing field values are null
            reject rows with all null fields
            (
		statefips,
		countyfips,
		countyname
	    )
	)
	LOCATION('countyfipsout.csv')
)
 REJECT LIMIT UNLIMITED;

CREATE TABLE ext_census_pop_estimate_load(
statefips		VARCHAR2(2 CHAR),
countyfips		VARCHAR2(5 CHAR),
pop2010			NUMBER,
pop2019			NUMBER
)
    ORGANIZATION EXTERNAL
    (
        TYPE ORACLE_LOADER
        DEFAULT DIRECTORY ext_data_dir
        ACCESS PARAMETERS
        (
            records delimited by '\n'
            readsize 2777834
            skip 1
            badfile ext_bad_dir:'popestimate.bad'
            logfile ext_log_dir: 'popestimate.log'
            fields terminated by ',' optionally enclosed by '"' NOTRIM
            missing field values are null
            reject rows with all null fields
            (
		statefips,
		countyfips,
		pop2010,
		pop2019
	    )
	)
	LOCATION('census2019out.csv')
)
 REJECT LIMIT UNLIMITED;

CREATE TABLE ext_covid_data_load(
statefips		VARCHAR2(2 CHAR),
countyfips		VARCHAR2(5 CHAR),
reportdate		DATE,
quantity		NUMBER,
type			VARCHAR2(1 CHAR)
)
    ORGANIZATION EXTERNAL
    (
        TYPE ORACLE_LOADER
        DEFAULT DIRECTORY ext_data_dir
        ACCESS PARAMETERS
        (
            records delimited by '\n'
            readsize 2777834
            skip 1
            badfile ext_bad_dir:'coviddata.bad'
            logfile ext_log_dir: 'coviddata.log'
            fields terminated by ',' optionally enclosed by '"' NOTRIM
            missing field values are null
            reject rows with all null fields
            (
		statefips,
		countyfips,
		reportdate DATE "YYYY-MM-DD",
		quantity,
		type
	    )
	)
	LOCATION('casesout_new.csv', 'deathsout_new.csv')
)
 REJECT LIMIT UNLIMITED;


