import pandas as pd
import numpy as np
import sys

inFile = sys.argv[1]
outFile1 = sys.argv[2]
outFile2 = sys.argv[3]


data = pd.read_csv(inFile, sep="|", header=None, dtype={10: str}, converters={13: lambda x: str(x)})

# copying data into another dataframe named df for creation of medianvals_by_zip.txt file
df=data.copy()                           
df=df.rename(columns = {0:'Id'})      
df=df.rename(columns = {10:'Zip'})               #Renaming important columns for usage in the later part of the code
df=df.rename(columns = {13:'Tdate'})
df=df.rename(columns = {14:'Tamt'})
df=df.rename(columns = {15:'Other'})
df['Zip'] = df['Zip'].astype(str).str[:5]                #Taking forst 5 digits of Zip code
df.dropna(subset=['Id'], how='all', inplace = True)      #Dropping rows with empty Id value
df.dropna(subset=['Tamt'], how='all', inplace = True)    #Dropping rows with empty Transaction amount value
df=df[pd.isnull(df['Other'])]                            #Including rows with empty Other value
df1= df[['Id', 'Zip', 'Tdate','Tamt']].copy()            #creating data frame for analysis
df2=df1.sort_values(by=['Zip'])                          #sorting by Zip Code to form groups
df3 = df2.copy()
a=df1.groupby('Zip')
for x in a.groups:
    a.get_group(x)
x = df3.Zip.unique()                                     #Taking only unique Zip Codes for calculating running Median of Amount
df4=pd.Series()
df6=pd.Series()

for i in range(len(x)):                                  #Computing Running Median of Transaction Amount
    z = x[i]
    y = a.get_group(z)
    df3=pd.rolling_apply(y.set_index('Zip')['Tamt'], window=len(y), func=np.nanmedian, min_periods=1 )
    df4=df4.append(df3)
df5=pd.DataFrame(df6.append(df4, ignore_index=False))
df5=df5.apply(np.round)                                  #Rounding off the median values
df5.index.name = 'Zip'
df5=df5.reset_index()
df5.columns = ['Zip', 'Median']
df5=pd.concat([df5.set_index('Zip'),df2.set_index('Zip')], axis=1, join='inner').reset_index()    #Concatinating dataframes to have one data frame that stores Running Median as per Zip codes
df5['running total'] = df5.groupby(['Zip']).cumcount() + 1         #Calculating running occurance of number of transactions received at that Zip code
df5['Running Sum']= df5.groupby(['Zip'])['Tamt'].cumsum()          #Calculating running sum of transaction amount received at that Zip code
df5=df5[['Id', 'Zip', 'Median', 'running total', 'Running Sum']]
df5.iloc[:,2:]=df5.iloc[:,2:].astype(int)
df5.to_csv(outFile1, sep='|', header="", index=False)    # saving the text file



# Code for creating the medianvals_by_date.txt file
df6=data.copy()
df6=df6.rename(columns = {0:'Id1'})
df6=df6.rename(columns = {13:'Tdate1'})                       #Renaming important columns for usage in the later part of the code
df6=df6.rename(columns = {14:'Tamt1'})
df6=df6.rename(columns = {15:'Other1'})
df6.dropna(subset=['Id1'], how='all', inplace = True)         #Dropping rows with empty Id value
df6.dropna(subset=['Tamt1'], how='all', inplace = True)       #Dropping rows with empty Transaction amount value
df6=df6[pd.isnull(df6['Other1'])]                             #Including rows with empty Other value
m = df6.groupby(['Id1'])[['Tamt1']].apply(np.median)
m=m.apply(np.round)                                           #Rounding off the median values
m.name = 'Median1'
df6=df6.join(m, on=['Id1'])                                   #Joining dataframes for further analysis
df7= df6[['Id1', 'Tdate1','Tamt1', 'Median1']].copy()
df7 = df7.drop_duplicates('Median1')                            
df8=df6.groupby(["Tdate1"]).size().reset_index(name="freq")   #Grouping by Transaction data for calculating total number of transactions received by recipient on that date
df9=pd.concat([df7.set_index('Tdate1'),df8.set_index('Tdate1')], axis=1, join='inner').reset_index()
a=df6.groupby('Tdate1')['Tamt1'].sum()                        #Calculating total amount of contributions received by recipient on that date
df12 = pd.DataFrame({'Total': a})
df12=df12.reset_index()
df12=pd.concat([df9.set_index('Tdate1'),df12.set_index('Tdate1')], axis=1, join='inner').reset_index() #Merging all dataframes
del df12['Tamt1']
df12.sort_values(by=['Id1', 'Tdate1'], ascending=[True, True])  #Sorting dataframe alphabetical by recipient and then chronologically by date
df12=df12[['Id1', 'Tdate1', 'Median1', 'freq', 'Total']]
df12.iloc[:,2:]=df12.iloc[:,2:].astype(int)
df12.to_csv(outFile2, sep='|', header="", index=False)  #Writing output to text file