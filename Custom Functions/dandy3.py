import pandas as pd
import math

def setlabelmethod(RC23,percent,quantile):
    return RC23.query('NameFilter!=""').\
    set_index('Label2').\
    join(RC23.drop_duplicates(subset=['RoomId','Label']).\
    assign(dummy=1).\
    groupby(['Label2','RoomId'])['dummy'].sum().\
    reset_index().\
    query('dummy<40 & dummy>1').\
    groupby(['Label2'])['dummy'].quantile(quantile).\
    reset_index().\
    assign(StarCount=lambda x: round(x.dummy))[['Label2','StarCount']].set_index('Label2')).\
    reset_index().set_index('RoomId').\
    join(RC23.\
        drop_duplicates(subset=['RoomId','Label']).\
        assign(dummy=1).\
        groupby(['RoomId'])['dummy'].sum().\
        reset_index().\
        set_index('RoomId')).\
        reset_index().\
    query('dummy>=StarCount').\
    drop_duplicates(subset=['RoomId','Label']).\
    groupby(['Label2','RoomId'])['Label'].apply(lambda x: ', '.join(x)).\
    reset_index().assign(dummy=1).groupby(['Label2','Label'])['dummy'].sum().\
    reset_index().\
    sort_values(['Label2','dummy'],ascending=False).groupby('Label2').head(1).set_index('Label2').join(RC23.\
    set_index('Label2').\
    join(RC23.drop_duplicates(subset=['RoomId','Label']).\
    assign(dummy=1).\
    groupby(['Label2','RoomId'])['dummy'].sum().\
    reset_index().\
    query('dummy<40 & dummy>1').\
    groupby(['Label2'])['dummy'].quantile(quantile).\
    reset_index().\
    assign(StarCount=lambda x: round(x.dummy))[['Label2','StarCount']].set_index('Label2')).\
    reset_index().set_index('RoomId').\
    join(RC23.\
        drop_duplicates(subset=['RoomId','Label']).\
        assign(dummy=1).\
        groupby(['RoomId'])['dummy'].sum().\
        reset_index().\
        set_index('RoomId')).\
        reset_index().\
    query('dummy>=StarCount').\
    drop_duplicates(subset=['RoomId','Label']).\
    groupby(['Label2','RoomId'])['Label'].apply(lambda x: ', '.join(x)).\
    reset_index().\
    assign(dummy=1).groupby(['Label2'])['dummy'].sum().\
    reset_index().\
    query('dummy>=20').\
    rename(columns={'dummy':'StarSetCount'}).\
    set_index('Label2'),how='inner').\
    assign(Percent= lambda x: x.dummy/x.StarSetCount).\
    query('Percent>='+str(percent)).reset_index()[['Label2','Label']].\
    assign(Label=lambda x: x.Label.str.split(',')).\
    explode('Label')



def setNFmethod(RC23,percent,quantile):
    return RC23.query('NameFilter!=""').\
    set_index('Label2').\
    join(RC23.drop_duplicates(subset=['RoomId','NameFilter']).\
    assign(dummy=1).\
    groupby(['Label2','RoomId'])['dummy'].sum().\
    reset_index().\
    query('dummy<40 & dummy>1').\
    groupby(['Label2'])['dummy'].quantile(quantile).\
    reset_index().\
    assign(StarCount=lambda x: round(x.dummy))[['Label2','StarCount']].set_index('Label2')).\
    reset_index().set_index('RoomId').\
    join(RC23.\
        drop_duplicates(subset=['RoomId','NameFilter']).\
        assign(dummy=1).\
        groupby(['RoomId'])['dummy'].sum().\
        reset_index().\
        set_index('RoomId')).\
        reset_index().\
    query('dummy>=StarCount').\
    drop_duplicates(subset=['RoomId','NameFilter']).\
    groupby(['Label2','RoomId'])['NameFilter'].apply(lambda x: ', '.join(x)).\
    reset_index().assign(dummy=1).groupby(['Label2','NameFilter'])['dummy'].sum().\
    reset_index().\
    sort_values(['Label2','dummy'],ascending=False).groupby('Label2').head(1).set_index('Label2').join(RC23.\
    set_index('Label2').\
    join(RC23.drop_duplicates(subset=['RoomId','NameFilter']).\
    assign(dummy=1).\
    groupby(['Label2','RoomId'])['dummy'].sum().\
    reset_index().\
    query('dummy<40 & dummy>1').\
    groupby(['Label2'])['dummy'].quantile(quantile).\
    reset_index().\
    assign(StarCount=lambda x: round(x.dummy))[['Label2','StarCount']].set_index('Label2')).\
    reset_index().set_index('RoomId').\
    join(RC23.\
        drop_duplicates(subset=['RoomId','NameFilter']).\
        assign(dummy=1).\
        groupby(['RoomId'])['dummy'].sum().\
        reset_index().\
        set_index('RoomId')).\
        reset_index().\
    query('dummy>=StarCount').\
    drop_duplicates(subset=['RoomId','NameFilter']).\
    groupby(['Label2','RoomId'])['NameFilter'].apply(lambda x: ', '.join(x)).\
    reset_index().\
    assign(dummy=1).groupby(['Label2'])['dummy'].sum().\
    reset_index().\
    query('dummy>=20').\
    rename(columns={'dummy':'StarSetCount'}).\
    set_index('Label2'),how='inner').\
    assign(Percent= lambda x: x.dummy/x.StarSetCount).\
    query('Percent>='+str(percent)).reset_index()[['Label2','NameFilter']].\
    assign(NameFilter=lambda x: x.NameFilter.str.split(',')).\
    explode('NameFilter')