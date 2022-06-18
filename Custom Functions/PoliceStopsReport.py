#run police report

def runreport(a): #function to run report
    import pandas as pd
    filepath='C:/Users\dmckenzie\Downloads\Police_Stop_Data.csv'
    DF=pd.read_csv(filepath,low_memory=False)
    DF['responseYear']=DF['responseDate'].str[0:4]
    StopsReport2021=DF.query('responseYear=="2021"').\
    reset_index(drop=True).\
    assign(dummy=1).\
    groupby(['reason','problem'])['dummy'].\
    sum().\
    reset_index().\
    rename(columns={'dummy':'Total',
                   'reason':'Reason',
                   'problem':'Problem'}).assign(Year=2021)[['Year','Reason','Problem','Total']]
    StopsReport2021.to_csv('C:/Users\dmckenzie\OneDrive - DLR Group\Desktop\StopsReport2021.csv')
    return print('Report Is Ready Saved To Desktop')
