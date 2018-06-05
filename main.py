from random import randint
import create_data          ## User defined module being imported here
import numpy as np
import os
import pandas as pd
import time

def calcBillingSummary(resultList):
    # Build a dataframe out of the billing records
    df = pd.DataFrame(data=np.vstack(sorted(resultList)))

    # Assign NEW column headers
    df.columns=['billingAcctNbr', 'billingPeriod', 'callerState', 'callerNbr', 'calledState', 'calledNbr', 'inout',  
                'startDateTime', 'endDateTime', 'withinPlan', 'duration', 'callPerMinute', 'callTotal']
       
    # Summarize on billing account number and get a count of mapped phone numbers
    dfSummaryCount = df.groupby(['billingAcctNbr'])['billingAcctNbr'].count()
    
    # Project from the dataframe columns needed for grouping
    dfProjection = df[['billingAcctNbr', 'billingPeriod', 'callerState', 'calledState', 'startDateTime', 'inout',
                       'withinPlan', 'duration', 'callPerMinute', 'callTotal']]
    
    # Use groupby and sum to group and aggregate data
    results  = dfProjection.groupby(['billingAcctNbr','billingPeriod','callerState', 'calledState', 'startDateTime', 'inout',
                                     'withinPlan','callPerMinute']).sum()

    return results

if __name__ == '__main__':
        
    # Setup configuration
    outDir  = 'C:\\Windows\\Temp\\VerizonSample\\reports\\'
    
    if not os.path.exists(outDir): os.makedirs(outDir)
    
    outFile = outDir + 'BillingSummary.csv'
    states  = create_data.setupStateConfig()

    # Dummy Random Billing Account Number vs linked or mapped phone numbers
    billingAcctNbrDict = {}
    for i in range(1,1000):
        while 1:
            key         = chr(randint(65,90)) + str(randint(10**5, (10**6)-1))
            callerNbr   = randint(10**9, (10**10)-1)
            if str(callerNbr)[:3] in states: break
                
        if key not in billingAcctNbrDict: 
            billingAcctNbrDict[key] = [callerNbr]
        else:
            toggle = randint(1,5)
            if len(billingAcctNbrDict[key]) < toggle:
                billingAcctNbrDict: billingAcctNbrDict[key].append(callerNbr)
    
##    for key in sorted(list(billingAcctNbrDict.keys())):
##        print(key,billingAcctNbrDict[key])
    
    # Create a list of lists containing billing record data
    yearList   = [2018]
    monthList  = [1,2,3,4,5]
    resultList = create_data.createBillingData(billingAcctNbrDict,yearList,monthList,2)

    # Calculate the billing summary
    results = calcBillingSummary(resultList)

    # Write out the billing summary to file
    try:
        results.to_csv(outFile)
        if os.path.exists(outFile):
            print(f'The {outFile} file was created')
        else:
            print(f'The {outFile} file could not be created')
    except IOError:
        print('There was issue writing the dataframe to a file')
    
    
