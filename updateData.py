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

for i in datadict:
        newdatadict[i]=[]
        for j in datadict[i]['features']:
            newdatadict[i].append(j['attributes'])
dfs={}
for i in counties:  #iterates through all the collected data and makes the cumulative data noncumulative
    df= pd.DataFrame(newdatadict[i])
    df.Date = pd.to_datetime(df.Date,unit='ms').dt.date
    df["Confirmed"] = df.supressed_cumulative_positive.fillna(0).diff()
    df["Probable"] = df.supressed_cumulative_probable.fillna(0).diff()
    df["FIPS"]=fipscodes[i]
    dfs[i]=df[["Date","County","Confirmed","Probable","FIPS",'Longitud','Latitude']].dropna()

df=pd.concat(dfs.values(),ignore_index=True)
df["Confirmed"]=df["Confirmed"].astype(int)
df["Probable"]=df["Probable"].astype(int)


#df[["Date","County","FIPS","Confirmed","Probable"]]


df=df[(~df.Confirmed<0)|(~df.Probable<0)] #sometimes DHEC uses a placeholder value that is negative, we don't want those

#Uncomment these lines and add a filepath to save to

#olddf = pd.read_csv("INSERT FILEPATH HERE")

#if not df.equals(olddf):
#    df.to_csv("INSERTFILEPATH",index=False)
#    print(df)
#    print(olddf)
