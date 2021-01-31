# covid19_review

<h3> Covid19 Review - Cases & Deaths in the United States</h3>

<p> The Covid19 pandemic has had a large impact in the country throughout
    the year.  As covid19 has progressed, data tracking its impact has
    proliferated.  I make use of that data to analyze the impact across
    time.  This repo brings together that data and the programs I have 
    created to facilitate that analysis.
</p>
    
<p> The data used for the analysis includes the following:</p>
    <ul>
    <li> Census Bureau Region, Division, and FIPS codes for states, 
         (state-geocodes-v2019.xlsx).</li>
    <li> Census Bureau Geography Vintage 2018
         (all-geocodes-v2019.xlsx).</li>
    <li> Census Bureau Population Estimates 2019, (co-est2019-alldata.csv).</li>
    <li> CDC annual mortality statistics encompassing all causes of deaths, 
         years 2015-2018 (Deaths2015.txt, Deaths2016.txt, Deaths2017.txt, 
         Deaths2018.txt) </li>
    <li> USA Facts (.org) covid-19 case and death counts (retrieved directly
        by python). </li>
    </ul>
    
<p> This repo also contains a set of programs developed to take the data above and 
    make it available for analysis.  The programs consist of both Python and
    Oracle code as provided below.<p>
    
<h4> Python</h4>    
<p> The file generateCovidData.py contains all the python code used to transform
    the data.  The end-product of this code is a set of output files that are used
    by the Oracle database programs detailed in the subsequent section.  The following 
    is a short description of the code found therein.</p>
    <ul>
    <ol>
        <li> generateCensusRegions: This loads the file all-geocodes-v2019.xlsx
             and generates an output file that will contain Census Bureau region and division, 
             plus state fips codes.</li>
        <li> generateCensusCounties: This loads the file state-geocodes-v2019.xlsx 
             and creates an output file of state counties by fips code.</li>
        <li> generatePopData: This loads the file co-est2019-alldata.csv and 
             generates a output file of census population estimates for all counties.</li>
        <li> generateDeathsData: This loads mortality files from the CDC that 
             encompass all causes of deaths and generates one output file containing 
             all the statistics for years 2015-2018.
        <li> generateCovidDaily - This receives a file (from 6 below) and transforms 
             that file into a daily count of either cases or deaths and writes it 
             out to file.</li> 
        <li> retrieveCovidData - This retrieves a couple of csv files from an 
             external website and calls the function (in 5 above) that transforms 
             those files into daily counts of confirmed cases or deaths.</li>
    </ol>
    </ul>
<h4> Oracle </h4>
<p> The following files contain all the code necessary to create the schema objects as
    well as process the data.  This code makes use of Oracle's external table functionality
    that enables read-only access to the external data files provided by Python.  </p>
    <ul>
    <ol>
        <li> createCovidTables.sql: This creates all the internal tables used to contain 
             the data created by the database package.  It also provides the external table
             definitions that support database query access to the data files forthcoming 
             from the python code above.</li>
        <li> createFactsView.sql: Generates a database view that brings together all the
             data contained in the individual internal tables.</li>
        <li> covid19-pak.sql: This generates a database package that contains all the
             code needed to process the data in the files provided by python.</li>
    </ol>
    </ul>
<p> Note: Please reference the Oracle Database Administrator's Guide for specifics on
    needed setup steps prior to using Oracle's external tables functionality.</p>
