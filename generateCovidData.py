import pandas as pd
import numpy as np
from pandas import Series,DataFrame
from datetime import date
from dateutil.parser import parse
import operator
import os

def fillzeros(var, size=5):

    return var.zfill(size)

def splitCounty(var):
    '''
        receive countyname and remove "County/Parish" from
        the var label and return after stripping 
        leading/trailings whitespaces
    '''
    if 'County' in var:
        var = var.split('County')[0]
    elif 'Parish' in var:
        var = var.split('Parish')[0]
    else:
        pass
    
    return var.strip().title()

def splitLabel(var,label):
    '''
    Receive a variable to split using
    the label as the basis for the split.
    Capitalize the var and return.
    '''
    
    newLabel = var.split(label)[0]

    
    return newLabel.strip().title()    
    
# create dictionary of USPS state code => statename

states = {'OH': 'Ohio', 'KY': 'Kentucky', 'NV': 'Nevada', 
      'WY': 'Wyoming', 'AL': 'Alabama', 'MD': 'Maryland', 
      'AK': 'Alaska', 'UT': 'Utah', 'OR': 'Oregon', 'MT': 'Montana', 
      'IL': 'Illinois', 'TN': 'Tennessee', 'DC': 'District of Columbia', 
      'VT': 'Vermont', 'ID': 'Idaho', 'AR': 'Arkansas', 'ME': 'Maine', 
      'WA': 'Washington', 'HI': 'Hawaii', 'WI': 'Wisconsin', 'MI': 'Michigan', 
      'IN': 'Indiana', 'NJ': 'New Jersey', 'AZ': 'Arizona', 
      'MS': 'Mississippi', 'PR': 'Puerto Rico', 'NC': 'North Carolina', 
      'TX': 'Texas', 'SD': 'South Dakota', 
      'IA': 'Iowa', 'MO': 'Missouri', 'CT': 'Connecticut', 'WV': 'West Virginia', 
      'SC': 'South Carolina', 'LA': 'Louisiana', 'KS': 'Kansas', 'NY': 'New York', 
      'NE': 'Nebraska', 'OK': 'Oklahoma', 'FL': 'Florida', 'CA': 'California', 
      'CO': 'Colorado', 'PA': 'Pennsylvania', 'DE': 'Delaware', 'NM': 'New Mexico',
      'RI': 'Rhode Island', 'MN': 'Minnesota', 
      'NH': 'New Hampshire', 'MA': 'Massachusetts', 'GA': 'Georgia', 
      'ND': 'North Dakota', 'VA': 'Virginia'}

stateNames = dict()
for key,val in states.items():
    stateNames[val] = key

dataDir = 'covid19/'

def generateCensusRegions():
    '''
    load file containing census bureau region,division,statefips codes.
    Create two output files and write to disk:
            1) dictionary of statefips -> statecode
            2) file containing census region, division, state that is
               loaded into Oracle.
    '''
    
    #load file containing census bureau region,division,statefips codes 

    colnames = ['region','division','statefips','name']
    censusCodes = pd.read_excel(dataDir+'state-geocodes-v2019.xlsx',
                                names=colnames,
                                converters={'statefips': lambda x: str(x)},
                                skiprows=5)

    # build census regions df
    censusRegions = censusCodes[censusCodes.division==0].copy()
    censusRegions.insert(4,'regionname',censusRegions['name'].apply(splitLabel, label='Region'))
    censusRegions.drop(['statefips','name', 'division'], axis=1, inplace=True)
    censusRegions.set_index(np.arange(len(censusRegions)), inplace=True)

    # build census divisions df
    censusDivisions = censusCodes[(censusCodes['statefips']=='00') & (censusCodes['division']>0)].copy()
    censusDivisions.insert(4,'divisionname',censusDivisions['name'].apply(splitLabel, label='Division'))
    censusDivisions.drop(['statefips','name'], axis=1, inplace=True)
    censusDivisions.set_index(np.arange(len(censusDivisions)), inplace=True)

    # build census states df
    censusStates = censusCodes[(censusCodes['division']>0) & (censusCodes['statefips']!= '00')].copy()
    censusStates.set_index(np.arange(len(censusStates)), inplace=True)
    censusStates.rename(columns={'name': 'statename'}, inplace=True)

    # assign USPS state code based on stateNames dct
    censusStates['statecode'] = censusStates['statename'].apply(lambda x: stateNames[x])

    # create dict of statefips->state code
    stateDct = dict()
    for rec in censusStates[['statefips','statecode']].values:
        stateDct[rec[0]] = rec[1]

    # write dict out to file
    np.save(datadir+'dataOut/states_dict.npy', stateDct)
    # load dictionary of docid->full name
    #stateDct = np.load(datadir+'dataOut/states_dict.npy', allow_pickle=True).item()

    # concatenate all 3 dfs (regions, divisions, states) into 1 and write to file
    census_reg_div_st = pd.merge(pd.merge(censusRegions, censusDivisions, left_on='region', right_on='region'),\
                         censusStates, left_on=['region','division'], right_on=['region','division'])
    census_reg_div_st.to_csv(datadir+'dataOut/censusregdivstateout.csv', index=False)
    

def generateCensusCounties():
    '''
    Load census bureau geocodes for counties.
    Create 2 files that are written to disk:
        1) statefips-countyfips-county name file that is loaded
           into Oracle.
        2) a dictionary for countyfips -> countyname.
    '''
    
    # load census bureau v2019 geocodes file of statefips, countyfips, countyname codes

    colnames = ['sumlev','statefips','countyfips','countyname']
    censusGeos = pd.read_excel(dataDir+'all-geocodes-v2019.xlsx', 
                                skiprows=4,
                                usecols=[0,1,2,6],
                                names=colnames,
                                converters={'statefips': lambda x: str(x).zfill(2),
                                           'countyfips': lambda x: str(x).zfill(3),
                                           'countyname': splitCounty}
                              )

    # create df of county records only and reset index.
    censusCounty = censusGeos[censusGeos['sumlev']==50].copy()
    censusCounty.set_index(np.arange(len(censusCounty)), inplace=True)

    #convert countyfips to 5-char value
    censusCounty['countyfips'] = censusCounty['statefips']+censusCounty['countyfips']

    # drop sumlev column
    censusCounty.drop(['sumlev'], axis=1, inplace=True)

    # write-out df to file (exclude Puerto Rico geocodes)
    censusCounty[censusCounty.statefips != '72'].to_csv(datadir+'dataOut/countyfipsout.csv', index=False)

    # create dict of countyfips->county name
    countyDct = dict()
    for rec in censusCounty[censusCounty.statefips != '72'][['countyfips','countyname']].values:
        countyDct[rec[0]] = rec[1]

    # write dict out to file
    np.save(datadir+'dataOut/county_dict.npy', countyDct)
    # load dictionary of countyfips -> county name
    #countyDct = np.load(datadir+'dataOut/county_dict.npy', allow_pickle=True).item()

def generatePopData():
    '''
    Load census bureau population estimates.
    Write out the file of population estimates
    by state-countyfips for loading into Oracle.
    '''
    
    #load census data of population estimate

    df2019 = pd.read_csv(dataDir+'co-est2019-alldata.csv', 
                         low_memory=False, 
                         encoding='latin',
                         dtype={'SUMLEV': object,
                              'REGION': object,
                              'DIVISION': object,
                              'STATE': object,
                              'COUNTY': object})

    # rename cols to lower-case
    df2019.columns = [x.lower() for x in df2019.columns]

    # create subset of columns and drop sumlev col
    df2019 = df2019[df2019.sumlev=='050'][['sumlev','region','division','state',\
                                           'county','census2010pop','popestimate2019']]
    df2019.drop('sumlev', axis=1, inplace=True)

    # insert new column for 5-char countyfips number.
    df2019.insert(4, 'countyfips', df2019.state+df2019.county)

    # drop old county column and rename state col.
    df2019.drop('county', axis=1, inplace=True)
    df2019.rename(columns={'state': 'statefips'}, inplace=True)

    # output census to csv file
    df2019[['statefips','countyfips','census2010pop','popestimate2019']].rename\
                            (columns={'census2010pop': 'pop2010',\
                            'popestimate2019': 'pop2019'}).to_csv(datadir+'dataOut/census2019out.csv', index=False)
     
def generateCovidDaily(df, fileLabel='cases'):
    '''
    generate daily cases & deaths from usa facts website data
    Fcn arguments are:
        1) df = latest file from usa facts website, either cases or deaths.
        2) fileLabel : indicates whether cases or deaths are received. Label
                        is used to assemble the filename of the output written
                        to disk plus assign a label to each record.
    '''
    #datadir = '/home/quark/Documents/PythonPandas/7_Covid19/2021CovidProject/'
    
    # check for any columns containing any ALL NA's and drop
    df.dropna(axis=1, how='all', inplace=True)

    # check for any rows containing any NA's and drop
    df.dropna(axis=0, how='any', inplace=True)

    # create list of columns in date format starting from
    # first daily date in df.
    caseCols = [] 
    for col in df.columns[4:]:
        caseCols.append(parse(col).date())

    # insert additional column names to list
    caseCols.insert(0,'countyfips')
    caseCols.insert(1, 'countyname')
    caseCols.insert(2, 'state')
    caseCols.insert(3, 'statefips')

    # rename df columns
    df.columns = caseCols

    # zero-fill columns
    df['countyfips'] = df['countyfips'].astype(str).apply(fillzeros,size=5)
    df['statefips'] = df['statefips'].astype(str).apply(fillzeros, size=2)

    # drop the countyname and state cols
    df.drop(['countyname','state'], axis=1, inplace=True)

    # create new df that converts cumulative values into daily values
    dfdaily = pd.merge(df.iloc[:, :3], df.iloc[:, 2:].diff(axis=1).drop(caseCols[4], \
                    axis=1), left_index=True, right_index=True)

    # put all date columns into rows and reset index
    dfdaily = dfdaily.set_index(['statefips','countyfips']).stack().reset_index()

    # rename columns
    dfdaily.rename(columns={'level_2': 'reportdate',
                           0: 'quantity'}, inplace=True)

    # convert quantity to int dtype
    dfdaily['quantity'] = dfdaily['quantity'].astype('int')

    # add label column that indicates (c)-cases or (d)-deaths based
    # on fileLabel value
    
    if fileLabel == 'cases':
        dfdaily['type'] = 'c'
    else:
        dfdaily['type'] = 'd'

    # write df to file
    filename = fileLabel+'out_new.csv'
    dfdaily.to_csv('dataOut/'+filename, index=False)
    
    #return dfdaily
    
def retrieveCovidData():
    '''
    Retrieve cases and deaths data from usa facts website.
    Call generateCovidDaily fcn to generate the daily counts
    for each defined df.
    '''
    
    # retrieve covid19 cases from usa facts website
    df_cases = pd.read_csv('https://usafactsstatic.blob.core.windows.net/public/data/covid-19/covid_confirmed_usafacts.csv',\
                            converters={'countyFIPS': lambda x: str(x).zfill(5),
                                        'stateFIPS':  lambda x: str(x).zfill(2)},
                            low_memory=False)
    
    # call to generate covid19 cases output file
    generateCovidDaily(df_cases, fileLabel='cases')
    
    
    # retrieve covid19 deaths from usa facts website
    df_deaths = pd.read_csv('https://usafactsstatic.blob.core.windows.net/public/data/covid-19/covid_deaths_usafacts.csv',\
                            converters={'countyFIPS': lambda x: str(x).zfill(5),
                                        'stateFIPS':  lambda x: str(x).zfill(2)},
                            low_memory=False) 
    
    # call to generate covid19 deaths output file
    generateCovidDaily(df_deaths, fileLabel='deaths')
    
