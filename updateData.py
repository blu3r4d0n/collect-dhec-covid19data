import pandas as pd
import requests
from zoneinfo import ZoneInfo
from datetime import timedelta,datetime


zone = ZoneInfo("US/Eastern")
today = datetime.now(tz=zone)

if today.weekday() != 6 or today.weekday() != 0:
    enddate = (today-timedelta(2))
elif today.weekday() == 6:
    #enddate = (today-timedelta(3))
    exit()
elif today.weekday() == 0:
    exit()
    #enddate = (today-timedelta(4))


#dates = pd.date_range("2020-21-24",enddate.strftime("%Y-%m-%d"))

counties=['Abbeville', 'Aiken', 'Allendale', 'Anderson', 'Bamberg', 'Barnwell', 'Beaufort', 'Berkeley', 'Calhoun', 'Charleston', 'Cherokee', 'Chester', 'Chesterfield', 'Clarendon', 'Colleton', 'Darlington', 'Dillon', 'Dorchester', 'Edgefield', 'Fairfield', 'Florence', 'Georgetown', 'Greenville', 'Greenwood', 'Hampton', 'Horry', 'Jasper', 'Kershaw', 'Lancaster', 'Laurens', 'Lee', 'Lexington', 'Marion', 'Marlboro', 'McCormick', 'Newberry', 'Oconee', 'Orangeburg', 'Pickens', 'Richland', 'Saluda', 'Spartanburg', 'Sumter', 'Union', 'Williamsburg', 'York']
datadict={}
for i in counties:
            datadict[i]=requests.get(f'https://services2.arcgis.com/XZg2efAbaieYAXmu/ArcGIS/rest/services/COVID_19_Time_Cases_Eastern_View/FeatureServer/0/query?where=County+%3D+%27{i}%27+AND+Date++BETWEEN+DATE+%272020-11-24%27+AND+DATE+%27{enddate.strftime("%Y-%m-%d")}%27&objectIds=&time=&geometry=&geometryType=esriGeometryEnvelope&inSR=&spatialRel=esriSpatialRelIntersects&resultType=none&distance=0.0&units=esriSRUnit_Meter&returnGeodetic=false&outFields=*&returnGeometry=true&featureEncoding=esriDefault&multipatchOption=xyFootprint&maxAllowableOffset=&geometryPrecision=&outSR=&datumTransformation=&applyVCSProjection=false&returnIdsOnly=false&returnUniqueIdsOnly=false&returnCountOnly=false&returnExtentOnly=false&returnQueryGeometry=false&returnDistinctValues=false&cacheHint=false&orderByFields=&groupByFieldsForStatistics=&outStatistics=&having=&resultOffset=&resultRecordCount=&returnZ=false&returnM=false&returnExceededLimitFeatures=true&quantizationParameters=&sqlFormat=none&f=pjson&token=').json()
newdatadict={}
fipscodes = {
"Abbeville": "45001",
"Aiken": "45003",
"Allendale": "45005",
"Anderson": "45007",
"Bamberg": "45009",
"Barnwell": "45011",
"Beaufort": "45013",
"Berkeley": "45015",
"Calhoun": "45017",
"Charleston": "45019",
"Cherokee": "45021",
"Chester": "45023",
"Chesterfield": "45025",
"Clarendon": "45027",
"Colleton": "45029",
"Darlington": "45031",
"Dillon": "45033",
"Dorchester": "45035",
"Edgefield": "45037",
"Fairfield": "45039",
"Florence": "45041",
"Georgetown": "45043",
"Greenville": "45045",
"Greenwood": "45047",
"Hampton": "45049",
"Horry": "45051",
"Jasper": "45053",
"Kershaw": "45055",
"Lancaster": "45057",
"Laurens": "45059",
"Lee": "45061",
"Lexington": "45063",
"Marion": "45067",
"Marlboro": "45069",
"McCormick": "45065",
"Newberry": "45071",
"Oconee": "45073",
"Orangeburg": "45075",
"Pickens": "45077",
"Richland": "45079",
"Saluda": "45081",
"Spartanburg": "45083",
"Sumter": "45085",
"Union": "45087",
"Williamsburg": "45089",
"York": "45091"
}
pop = {'45001': 24295,
 '45003': 168808,
 '45005': 8039,
 '45007': 203718,
 '45009': 13311,
 '45011': 20589,
 '45013': 187117,
 '45015': 229861,
 '45017': 14119,
 '45019': 408235,
 '45021': 56216,
 '45023': 32294,
 '45025': 43273,
 '45027': 31144,
 '45029': 38604,
 '45031': 62905,
 '45033': 28292,
 '45035': 161540,
 '45037': 25657,
 '45039': 20948,
 '45041': 137059,
 '45043': 63404,
 '45045': 525534,
 '45047': 69351,
 '45049': 18561,
 '45051': 351029,
 '45053': 28791,
 '45055': 65403,
 '45057': 96016,
 '45059': 67539,
 '45061': 16531,
 '45063': 289886,
 '45065': 9526,
 '45067': 29183,
 '45069': 26667,
 '45071': 37719,
 '45073': 78607,
 '45075': 84223,
 '45077': 131404,
 '45079': 416147,
 '45081': 22967,
 '45083': 327997,
 '45085': 105556,
 '45087': 27244,
 '45089': 31026,
 '45091': 282090}

for i in datadict:
        newdatadict[i]=[]
        for j in datadict[i]['features']:
            newdatadict[i].append(j['attributes'])
dfs={}
for i in counties:
    df= pd.DataFrame(newdatadict[i])
    df.Date = pd.to_datetime(df.Date,unit='ms').dt.date
    df["Confirmed"] = df.supressed_cumulative_positive.fillna(0).diff()
    df["Probable"] = df.supressed_cumulative_probable.fillna(0).diff()
    df["FIPS"]=fipscodes[i]
    df["pop2020"]=pop[fipscodes[i]]
    dfs[i]=df[["Date","County","Confirmed","Probable","FIPS","pop2020",'Longitud','Latitude']].dropna()

df=pd.concat(dfs.values(),ignore_index=True)
df["Confirmed"]=df["Confirmed"].astype(int)
df["Probable"]=df["Probable"].astype(int)


#df[["Date","County","FIPS","Confirmed","Probable"]]



## put your filepath on the commentted lines
df=df[(~df.Confirmed<0)|(~df.Probable<0)]
#olddf = pd.read_csv("FILEPATH HERE")
print(df.equals(olddf))
if not len(df) == len(olddf):  #this just checks to see if the data is the same as the last time the script ran
#    df.to_csv("FILEPATH HERE",index=False)
    print(df)
    print(olddf)
