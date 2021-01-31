CREATE OR REPLACE VIEW VW_COVID_FACTS
AS
SELECT qry1.regionname, 
                qry1.divisionname, 
                qry1.statefips, 
                qry1.statecode, 
                qry1.countyfips, 
                qry1.countyname, 
                cases.reportdate, 
                cases.quantity AS cases, 
                deaths.quantity AS deaths
FROM
    (SELECT a.regionid, 
                    a.regionname, 
                    b.divisionid, 
                    b.divisionname, 
                    d.statefips,
                    d.statecode, 
                    e.countyfips, 
                    e.countyname
    FROM cov_regions a 
    JOIN cov_regionsdivisions c ON (a.regionid=c.regionid)
    JOIN cov_divisions b ON (b.divisionid = c.divisionid)
    JOIN cov_fipsstate d ON (c.statefips = d.statefips)
    JOIN cov_fipscounty e ON (d.statefips = e.statefips)
    ) qry1 
    JOIN cov_cases cases ON (qry1.statefips = cases.statefips 
                             AND qry1.countyfips=cases.countyfips)
    JOIN cov_deaths deaths ON (cases.statefips = deaths.statefips 
                               AND cases.countyfips=deaths.countyfips 
                               AND cases.reportdate = deaths.reportdate);
