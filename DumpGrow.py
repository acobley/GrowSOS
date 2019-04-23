# This code reads from the Thingful node, gets  list of all Sensors
# The code then reads through the list of sensors and counts the number with a location of
# zero (at the moment assumes if x=0 y=0)
# Then displays percentage that are zero locations
# Documentation for GROW node
# https://growobservatory.github.io/ThingfulNode/#tag/Locations
#
#
#

import requests, json, sys
from requests.auth import HTTPBasicAuth
import csv
from SecretHL import AuthCode
import threading
from time import sleep
from threading import Lock
lock = Lock()
global ThreadCount
fileCsv = csv.writer(open("GrowTimeSeries.csv", "w"))
fileCsv.writerow(["Serial","Device","sReadingType", "sActualValue", "sTime"])
AllCsv=csv.writer(open("All.csv", "a"))
ThreadCount=0




def TimeLine(Serial,Code,BeginTime,EndTime,sObservableProperty):

    global ThreadCount
    Device=Code
    Begin=BeginTime
    End=EndTime
    Type=sObservableProperty

    url="http://grow-beta-api.hydronet.com/api/service/sos?service=SOS&responseformat=application/JSON&request=getobservation&procedure="+Device+"&Version=2.0.0&ObservedProperty="+Type+"&temporalfilter=phenomenonTime,"+Begin+"/"+End
    print("{0},{1}".format(ThreadCount,url))
    headers = {"Authorization": "Bearer {0}".format(AuthCode)}
    payload = json.dumps({})

    response = requests.get(url, auth=HTTPBasicAuth('.\GROW_HL', '321Demo'))
    # exit if status code is not ok
    if response.status_code != 200:
      print("Unexpected response: {0}. Status: {1}. ".format(response.reason, response.status_code))
      lock.acquire()
      ThreadCount = ThreadCount-1
      lock.release()
      return
    jResp = response.json()
    #print json.dumps(jResp,indent=4,sort_keys=True)
    contents=jResp["observationData"]
    #print json.dumps(contents, indent=4,sort_keys=True)

    for key in contents:
       #print json.dumps(key, indent=4,sort_keys=True)

       observedProperty=key["OM_Observation"]["observedProperty"]
       result=key["OM_Observation"]["result"]
       resultTime=key["OM_Observation"]["resultTime"]
       readingType=observedProperty["nilReason"]
       sReadingType=readingType.encode('ascii','ignore')
       iDot= sReadingType.rfind(".")+1
       oDot= slice(iDot,len(sReadingType),1)
       ReadingType=sReadingType[oDot]
       ActualValue=result["ActualValue"]
       sActualValue=ActualValue.encode('ascii','ignore')
       Time=resultTime["TimeInstant"]["timePosition"]["Value"]
       sTime=Time.encode('ascii','ignore')
       fileCsv.writerow([Serial,Device,ReadingType, sActualValue, sTime])
    lock.acquire()
    ThreadCount = ThreadCount-1
    lock.release()

    return



url = "http://grow-beta-api.hydronet.com/api/service/sos?service=SOS&request=getcapabilities&Version=2.0.0&ResponseFormat=application/JSON"
headers = {"Authorization": "Bearer {0}".format(AuthCode)}
payload = json.dumps({})
threads = []

response = requests.get(url, auth=HTTPBasicAuth('.\GROW_HL', '321Demo'))
# exit if status code is not ok
if response.status_code != 200:
  print("Unexpected response: {0}. Status: {1}. ".format(response.reason, response.status_code))
  sys.exit()

jResp = response.json()

#print json.dumps(jResp,indent=4,sort_keys=True)

contents=jResp["Contents"]
#print json.dumps(contents, indent=4,sort_keys=True)
CContents=contents["Contents"]
#print json.dumps(CContents, indent=4,sort_keys=True)
offering=CContents["offering"]
#print json.dumps(offering, indent=4,sort_keys=True)
#print ("Serial,Longitude,Latitude,Type")

for key in offering:
    AbstractOffering=key["AbstractOffering"]
    #print(json.dumps(AbstractOffering, indent=4,sort_keys=True))
    description=AbstractOffering["description"]
#    print description
    items=AbstractOffering["observedArea"]["Item"]["Items"]
    observableProperty=AbstractOffering["observableProperty"]
    for key2 in observableProperty:
        sObservableProperty=key2.encode('ascii','ignore')
    sDec=description.encode('ascii','ignore')
    iFor= sDec.find("for")
    iLocation=sDec.find("location")+9
    iSerial=sDec.find("Serial")
    oSlice= slice(iFor)
    lSlice=slice(iLocation,iSerial,1)
    sSlice=slice(sDec.find("Number:")+8,sDec.find("with")-1,1)
    sCode=slice(sDec.find("code")+5,len(sDec),1)
    Type=sDec[oSlice]
    Location=sDec[lSlice]
    Serial=sDec[sSlice]
    Code=sDec[sCode]

    for key2 in items:
       location=key2["Value"]
       location=location.encode('ascii','ignore')
       iSpace=location.find(" ")
       latSlice=slice(iSpace)
       Lat=location[latSlice]
       sLong=slice(iSpace+1,len(location),1)
       Long=location[sLong]
       SensorType="Flower Power"
    BeginTime=AbstractOffering["resultTime"]["TimePeriod"]["Item"]["Value"]
    EndTime=AbstractOffering["resultTime"]["TimePeriod"]["Item1"]["Value"]

    while (ThreadCount > 20):
       sleep(5)
       Locked+=1
       if Locked > 30:
          print("{0},{1}".format(ThreadCount,Locked))
          sys.exit()
    Locked=0


    t = threading.Thread(target=TimeLine, args=(Serial,Code,BeginTime,EndTime,sObservableProperty,))
    threads.append(t)
    t.start()
    ThreadCount=ThreadCount+1



    #TimeLine(Serial,Code,BeginTime,EndTime,sObservableProperty)
    AllCsv.writerow([Serial,Lat,Long,Type,SensorType,Code,BeginTime,EndTime])
    #print("{0},{1},{2},{3},{4},{5},{6},{7}".format(Serial,Lat,Long,Type,SensorType,Code,BeginTime,EndTime))
sys.exit()
