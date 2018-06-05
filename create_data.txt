"""
#################################################################################
This script can be used to create dummy and random billing data.
It takes 3 parameters and they are,

Billing Account Number :    It can be a billing account number for a family of 
                            phone numbers or an individual billing account for 
                            one phone number
Year                   :    The billing year (1950-Current Year)
Month                  :    The billing month (1-12)
#################################################################################
"""
from datetime import datetime
from multiprocessing import Pool, Process
from random import randint
import calendar
import math
import os
import shutil
import time

COSTOFCALLPERMINUTE = 0.01

def setupStateConfig():
    states = {}
    # Using built-in context manager
    with open('C:\\Windows\\Temp\\VerizonSample\\states.csv','r') as fh:
        records = fh.readlines()
    if not fh.closed: fh.close() # Not needed but for safety

    for rec in records:
        arr = rec.split(',')
        arr[-1] = arr[-1].strip()
        areacodeList = arr[-1].split('|')
        for areacode in areacodeList:
            states[areacode] = arr[1:]
    return states

def randomTimestamp(year=None,month=None,day=None):
    """
    This function creates and returns a random datetime
    """
    yyyy  = randint(1950,datetime.now().year)   if not year  else year
    mm    = randint(1,12)                       if not month else month
    dd    = None                                if not day   else day
    mmEnd = calendar.monthrange(yyyy,mm)[1]
    while not dd or (day and dd > day):
        dd = randint(1,31)
        if mmEnd < dd: dd = None
    hh    = randint(0,23)
    mi    = randint(0,59)
    ss    = randint(0,59)
    ms    = randint(0,(10**6)-1)
    return yyyy,mm,dd,hh,mi,ss,ms
    
def createBillingData(billingAcctNbrDict,yearList,monthList,poolSize=None):
    """
    This function creates a individual or consolidate account itemized billing records
    """
    resultList = []
    for year in yearList:
        for month in monthList:
            billingAcctNbrList = [(key,value,year,month) for key,value in billingAcctNbrDict.items()]
            p = Pool(1 if not poolSize else poolSize)
            resultList.extend(p.map(poolWorker,billingAcctNbrList))
    return resultList 

def poolWorker(arr):
    """
    This function is a worker pool function and is called by the 'createBillingData' function
    """
    key                 = arr[0]
    value               = arr[1]
    billingAcctNbrDict  = {key:value}
    year                = arr[2]
    month               = arr[3]
    outList             = []
    states              = setupStateConfig()
    
    prevbillingAcctNbr = None
    for item in sorted(billingAcctNbrDict.items()):
        
        billingAcctNbr = item[0]
        tmpList = []
        
        for callerNbr in item[1]:
            # Dummy Caller State
            callerState = states[str(callerNbr)[:3]][0] if str(callerNbr)[:3] in states else None
                        
            while 1: #Break out when calledState is set
                # Dummy Random Called Number
                calledNbr = randint(10**9, (10**10)-1)

                # Dummy Called State
                calledState = states[str(calledNbr)[:3]][0] if str(calledNbr)[:3] in states else None

                if calledState: break

                
            # Dummy call direction : 0->Incoming 1->Outgoing
            inout = randint(0,1)
            
            # Dummy start time and billing period
            startDateTime = None
            while not startDateTime:
                yyyy,mm,dd,hh,mi,ss,ms = randomTimestamp(year,month)
                startDateTime = datetime(yyyy,mm,dd,hh,mi,ss,ms)
                billingPeriod = str(yyyy) + str(mm).zfill(2)
                
            # Dummy end time but should be greater than startDateTime
            endDateTime = None
            while not endDateTime:
                yyyy,mm,dd,hh,mi,ss,ms = randomTimestamp(startDateTime.year,startDateTime.month,startDateTime.day)
                endDateTime = datetime(yyyy,mm,dd,hh,mi,ss,ms)
                if endDateTime < startDateTime or math.ceil((endDateTime - startDateTime).seconds/60) > 12:
                    endDateTime = None
                    continue
                
            # Dummy Random withinPlan determination : 0->False, 1->True
            withinPlan = randint(0,1)

            # Dummy Random duration calculated based on endDateTime - startDateTime
            duration = math.ceil((endDateTime - startDateTime).seconds / 60) # convert to minutes

            # Dummy Random callPerMinute
            callPerMinute = COSTOFCALLPERMINUTE if not withinPlan else 0

            # Dummy Random callTotal calculated based on duration * callPerMinute
            callTotal = (duration * callPerMinute) if not withinPlan else 0
            
            outList.append((billingAcctNbr, billingPeriod, callerState, callerNbr, calledState, calledNbr, inout,  
                            startDateTime, endDateTime, withinPlan, duration, callPerMinute, callTotal))
    return outList

###################################################################################################################
###################################################################################################################
###################################################################################################################
   
if __name__ == '__main__':

    # Dummy Random Billing Account Number vs linked or mapped phone numbers
    billingAcctNbrDict = {}
    states             = setupStateConfig()
    for i in range(1,10):
        while 1:
            key         = chr(randint(65,90)) * randint(1,9)
            callerNbr   = randint(10**9, (10**10)-1)
            if str(callerNbr)[:3] in states: break
                
        if key not in billingAcctNbrDict: 
            billingAcctNbrDict[key] = [callerNbr]
        else:
            toggle = randint(0,1)
            if toggle:
                if len(billingAcctNbrDict[key]) < 5:
                    billingAcctNbrDict: billingAcctNbrDict[key].append([callerNbr])
    
    # Create dummy billing data for individual
    
    # Using a single process and timing it
    stime = time.time()
    resultList = createBillingData(billingAcctNbrDict,[2018,],[1,2,3,4,5],None)
    etime = time.time()    
    timeTaken = etime - stime
    print(f'The time taken by a single process : {timeTaken}')

    stime = time.time()
    resultList = createBillingData(billingAcctNbrDict,[2018,],[1,2,3,4,5],2)
    etime = time.time()    
    timeTaken = etime - stime
    print(f'The time taken by multi processing : {timeTaken}')

    # Write the billing data to file for each account number
    fh                  = None
    prevbillingAcctNbr  = ''
    outDir              = 'C:\\Windows\\Temp\\VerizonSample\\data'

    if os.path.exists(outDir):
        shutil.rmtree(outDir)
        if os.path.exists(outDir):
            msg = f'The {outDir} directory still exists'
            raise Exception(msg)
    
    os.makedirs(outDir)
    
    if not os.path.exists(outDir):
        msg = f'The {outDir} directory does not exist'
        raise Exception(msg)
    
    for items in sorted(resultList):
        for item in items:
            (billingAcctNbr, billingPeriod, callerState, callerNbr, calledState, calledNbr, inout,  
             startDateTime, endDateTime, withinPlan, duration, callPerMinute, callTotal) = item
            data = ('%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s' % \
                  (billingAcctNbr, billingPeriod, callerState, callerNbr, calledState, calledNbr, inout,  
                   startDateTime, endDateTime, withinPlan, duration, callPerMinute, callTotal))
            if prevbillingAcctNbr != billingAcctNbr:
                prevbillingAcctNbr = billingAcctNbr
                if fh: fh.close()

                YYYYMM  = str(startDateTime.year) + str(startDateTime.month)
                outFile = outDir + '\\' + billingAcctNbr + '_' + YYYYMM + '.csv'
                header  = "billingAcctNbr,billingPeriod,callerState,callerNbr,calledState,calledNbr,inout,"
                header += "startDateTime,endDateTime,withinPlan,duration,callPerMinute,callTotal"
                
                fh = open(outFile, 'w')
                fh.write(header + '\n')
            fh.write(data + '\n')
    if fh: fh.close()
    
