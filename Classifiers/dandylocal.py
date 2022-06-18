import nltk
import re
import pandas as pd
import pyodbc 
from gensim.models import Word2Vec
import itertools
from nltk.corpus import wordnet as wn
from nltk.stem.wordnet import WordNetLemmatizer
from nltk import pos_tag
from collections import defaultdict
import pyodbc 
import numpy as np
import math
def tokenlenfilter(tokenlist,minlen=2):
    tokens = [x for x in tokenlist if len(x)>minlen]
    return (tokens)

def lemmer(text):
    lemma_function = WordNetLemmatizer()

    tag_map = defaultdict(lambda : wn.NOUN)
    tag_map['J'] = wn.ADJ
    tag_map['V'] = wn.VERB
    tag_map['R'] = wn.ADV

    return [lemma_function.lemmatize(token, tag_map[tag[0]]) for token, tag in pos_tag(text)]

def get_ngrams(series: pd.Series,n:int):
    ngrams = series.copy().str.split(' ').explode()
    ngrams1 = series.copy().str.split(' ').explode()
    for i in range(1,n):
        foo=ngrams1.groupby(level=0).shift(-i)
        ngrams=ngrams.str.cat(foo,sep=' ')
    ngrams = ngrams.dropna()
    return pd.DataFrame(ngrams)

def get_multigrams(series,n):
    grams=get_ngrams(series,1)
    if n==1:
        return grams
    else:
        for i in range(1,n+1):
            if i==1:
                continue
            grams=grams.append(get_ngrams(series,i))
    return grams

def camel_case_split(identifier):
    matches = re.finditer('.+?(?:(?<=[a-z])(?=[A-Z])|(?<=[A-Z])(?=[A-Z][a-z])|$)', identifier)
    return [m.group(0) for m in matches]

def tokenswap(tokenlist,oldtoken,newtoken):
    tokens=[]
    for x in tokenlist:
        if x==oldtoken:
            tokens.append(newtoken)
        else:
            tokens.append(x)
    
    return (tokens)

def tokennamefilter(tokenlist,uncommontokens,manufact,minlen=2):
    tokens = [x for x in tokenlist if x not in manufact+uncommontokens+['rm','room','men','women','mens','inmate','female','male',
                                                               'ada','accessible','walden','enscape','assetdefinition',
                                                                        'asset','definition','nested','element',
                                                                        'nelson','nestedelement','dividends','horizon',
                                                               'womens','girl','boy','boys','girls','detention','high','tower',
                                                              'east','west','north','south','space','area','zone',
                                                              'small','medium','large','xlarge','dividend',
                                                               'dlr','dlrj','hightower','generic','campfire','turnstone','inc',
              'coalesse','mooreco','norvanivel','dividends horizon','knoll','eames',
              'steelcase','dlrjz','bobrick','dlrz','bradley','hermanmiller','hospitality','acorn',
              'herman','miller','bms']+
             ['attenda','enea','silq','limerick','lenox','lottus','kathryn',
'akira','collection','lumencore','altzo','visalia','turnstone','cobi','ology','amia',
'soto','eames','ladena','thrive','montara','bindu','newton','massaud','walden','victaulic','versoleil','deja','titus',
'lyze','millbrae','montara','mirra','nelson','deja','archibald','tavju','aerada','amerlux']+
             ['nuevo','hondo','furn','det','detentionwallmounteddoublebunk']]
    return (tokens)

def get_rc_word2vec():
    
    conn = pyodbc.connect('Driver={SQL Server};'
                      'Server=DLRBIMSQL;'
                      'Database=ClarityIndex;'
                      'Trusted_Connection=yes;')
    cursor = conn.cursor()
    
    query = """ SELECT  Name
    FROM [ClarityIndex].[dbo].[PQ_RoomContents] PQRC
    group by Name  """

    RNS = pd.read_sql_query(query,conn)
    
    cursor.close()
    conn.close()

    RNS['Name2']=RNS['Name'].str.replace('\d+','').str.replace('\+','').str.replace('[^\w\s]',' ').str.replace('_',' ').\
        str.lstrip().str.rstrip('-').str.rstrip()
    RNS['Name2']=RNS['Name2'].str.replace('\.','').str.replace('\\',' ').str.replace('-',' ').str.replace('/',' ').str.replace("''",'')
    RNS['Name2']=RNS['Name2'].apply(camel_case_split).apply(' '.join).str.lstrip().str.lower()

    RNS['NameFilter']=RNS['Name2'].apply(nltk.word_tokenize).\
        apply(tokenlenfilter)

    for i  in RNS['NameFilter']:
        if len(i)<3:
            i.sort()
        
    RNS['NameFilter']=RNS['NameFilter'].apply(' '.join).str.lstrip()

    listforw2v=list(RNS[RNS['NameFilter']!=''].\
                    drop_duplicates(subset=['NameFilter'])['NameFilter'].\
                    apply(nltk.word_tokenize))

    model = Word2Vec(listforw2v, min_count=1)
    
    return model

def word2vecscore(topwords,mod):
    words2score=list(itertools.combinations(topwords,2))
    scores=[]
    for i in words2score:
        score=mod.wv.similarity(i[0],i[1])
        scores.append(score)
    
    coherence=np.median(scores)
    return coherence 


def RuleBasedRoomInputClassifier(SeriesRaw: pd.Series,SeriesLabs=False):
    if len(SeriesLabs)==1:
        SeriesLabs=SeriesRaw.copy()
    
    ReturnSeries=np.where((SeriesRaw.str.contains('|'.join(['phone']))==True) & 
             (SeriesRaw.str.contains('|'.join(['microphone']))==False),'phone',SeriesLabs)
    
    ReturnSeries=np.where((SeriesRaw.str.contains('|'.join(['cabinet']))==True),'cabinet',ReturnSeries)
    
    ReturnSeries=np.where((SeriesRaw.str.contains('|'.join(['cabinet']))==True) & 
             (SeriesRaw.str.contains('|'.join(['base']))==False),'base cabinet',ReturnSeries)
    
    ReturnSeries=np.where((SeriesRaw.str.contains('|'.join(['cabinet']))==True) & 
             (SeriesRaw.str.contains('|'.join(['upper']))==False),'upper cabinet',ReturnSeries)
    
    ReturnSeries=np.where((SeriesRaw.str.contains('|'.join(['tall']))==True) & 
             (SeriesRaw.str.contains('|'.join(['storage']))==False),'tall storage',ReturnSeries)
    
    
    
    ReturnSeries=np.where((SeriesRaw.str.contains('|'.join(['counter']))==True) & 
             (SeriesRaw.str.contains('|'.join(['top']))==False),'counter top',ReturnSeries)
    
    ReturnSeries=np.where((SeriesRaw.str.contains('|'.join(['counter']))==True) & 
             (SeriesRaw.str.contains('|'.join(['sink']))==False),'sink counter',ReturnSeries)
    ReturnSeries=np.where((SeriesRaw.str.contains('|'.join(['counter']))==True) & 
             (SeriesRaw.str.contains('|'.join(['splash']))==False),'counter splash',ReturnSeries)
    
    ReturnSeries=np.where((SeriesRaw.str.contains('|'.join(['dispenser']))==True) & 
             (SeriesRaw.str.contains('|'.join(['tissue']))==False),'tissue dispenser',ReturnSeries)
    
    ReturnSeries=np.where((SeriesRaw.str.contains('|'.join(['dispenser']))==True) & 
             (SeriesRaw.str.contains('|'.join(['soap']))==False),'soap dispenser',ReturnSeries)
    
    ReturnSeries=np.where((SeriesRaw.str.contains('|'.join(['wall']))==True) & 
             (SeriesRaw.str.contains('|'.join(['outlet']))==False),'wall outlet',ReturnSeries)
    
    ReturnSeries=np.where((SeriesRaw.str.contains('|'.join(['tele']))==True) & 
             (SeriesRaw.str.contains('|'.join(['outlet']))==False),'tele outlet',ReturnSeries)
    
    
    ReturnSeries=np.where((SeriesRaw.str.contains('|'.join(['desk']))==True),'desk',ReturnSeries)
    ReturnSeries=np.where((SeriesRaw.str.contains('|'.join(['cubby']))==True),'cubby',ReturnSeries)
    ReturnSeries=np.where((SeriesRaw.str.contains('|'.join(['stool']))==True),'stool',ReturnSeries)
    ReturnSeries=np.where((SeriesRaw.str.contains('|'.join(['clock']))==True),'clock',ReturnSeries)
    ReturnSeries=np.where((SeriesRaw.str.contains('|'.join(['camera']))==True),'camera',ReturnSeries)
    ReturnSeries=np.where((SeriesRaw.str.contains('|'.join(['mirror']))==True),'mirror',ReturnSeries)
    ReturnSeries=np.where((SeriesRaw.str.contains('|'.join(['bench']))==True),'bench',ReturnSeries)
    ReturnSeries=np.where((SeriesRaw.str.contains('|'.join(['chair']))==True),'chair',ReturnSeries)
    ReturnSeries=np.where((SeriesRaw.str.contains('|'.join(['seat']))==True),'seat',ReturnSeries)
    ReturnSeries=np.where((SeriesRaw.str.contains('|'.join(['exit']))==True),'exit',ReturnSeries)
    ReturnSeries=np.where((SeriesRaw.str.contains('|'.join(['desk']))==True),'desk',ReturnSeries)
    ReturnSeries=np.where((SeriesRaw.str.contains('|'.join(['table']))==True),'table',ReturnSeries)
    ReturnSeries=np.where((SeriesRaw.str.contains('|'.join(['rug']))==True),'rug',ReturnSeries)
    ReturnSeries=np.where((SeriesRaw.str.contains('|'.join(['copier']))==True),'copier',ReturnSeries)
    ReturnSeries=np.where((SeriesRaw.str.contains('|'.join(['downspout']))==True),'downspout',ReturnSeries)
    ReturnSeries=np.where((SeriesRaw.str.contains('|'.join(['down spout']))==True),'downspout',ReturnSeries)
    ReturnSeries=np.where((SeriesRaw.str.contains('|'.join(['damper']))==True),'damper',ReturnSeries)
    ReturnSeries=np.where((SeriesRaw.str.contains('|'.join(['mullion']))==True),'mullion',ReturnSeries)


    #note these ones
    ReturnSeries=np.where((SeriesRaw.str.contains('|'.join(['fence']))==True),'fence',ReturnSeries)
    ReturnSeries=np.where((SeriesRaw.str.contains('|'.join(['gate']))==True),'gate',ReturnSeries)
    ReturnSeries=np.where((SeriesRaw.str.contains('|'.join(['television']))==True),'television',ReturnSeries)

    ReturnSeries=np.where((SeriesRaw.str.contains('|'.join(['air condition']))==True),'air conditioner',ReturnSeries)
    ReturnSeries=np.where((SeriesRaw.str.contains('|'.join(['coffee maker']))==True),'coffee maker',ReturnSeries)
    ReturnSeries=np.where((SeriesRaw.str.contains('|'.join(['coffee machine']))==True),'coffee maker',ReturnSeries)
    ReturnSeries=np.where((SeriesRaw.str.contains('|'.join(['treadmill']))==True),'treadmill',ReturnSeries)
    ReturnSeries=np.where((SeriesRaw.str.contains('air') & SeriesRaw.str.contains('supply')),
                       'air supply',
                       ReturnSeries)

    ReturnSeries=np.where((SeriesRaw.str.contains('basket') & SeriesRaw.str.contains('goal')),
                       'basketball goal',
                       ReturnSeries)


    ReturnSeries=np.where((SeriesRaw.str.contains('basket') & SeriesRaw.str.contains('backboard')),
                       'basketball backboard',
                       ReturnSeries)


    ReturnSeries=np.where((SeriesRaw.str.contains('air') & SeriesRaw.str.contains('return')),
                       'air return',
                       ReturnSeries)

    ReturnSeries=np.where((SeriesRaw.str.contains('grab') & SeriesRaw.str.contains('horiz')),
                       'horizontal grab bar',
                       ReturnSeries)


    ReturnSeries=np.where((SeriesRaw.str.contains('grab') & SeriesRaw.str.contains('vert')),
                       'vert grab bar',
                       ReturnSeries)
    ReturnSeries=np.where((SeriesRaw.str.contains('paper') & SeriesRaw.str.contains('towel')),
                       'paper towel',
                       ReturnSeries)

    ReturnSeries=np.where((SeriesRaw.str.contains('parking') & SeriesRaw.str.contains('stall')),
                       'parking stall',
                       ReturnSeries)

    ReturnSeries=np.where((SeriesRaw.str.contains('nurse') & SeriesRaw.str.contains('call')),
                       'nurse call',
                       ReturnSeries)


    ReturnSeries=np.where((SeriesRaw.str.contains('|'.join(['xray','x ray','x-ray']))==True),'x ray',ReturnSeries)
    ReturnSeries=np.where((SeriesRaw.str.contains('table') & SeriesRaw.str.contains('medical')),
                       'medical table',
                       ReturnSeries)
    ReturnSeries=np.where((SeriesRaw.str.contains('|'.join('medical')) & SeriesRaw.str.contains('bed')),
                       'medical bed',
                       ReturnSeries)
    ReturnSeries=np.where((SeriesRaw.str.contains('drinking') & SeriesRaw.str.contains('fountain')),
                       'drinking fountain',
                       ReturnSeries)
    ReturnSeries=np.where(SeriesRaw.str.contains('|'.join(['desk'])) & 
                       SeriesRaw.str.contains('|'.join(['wall','mtd','mount','detention'])),
                       'desk detention',
                       ReturnSeries)
    ReturnSeries=np.where(SeriesRaw.str.contains('|'.join(['shelf','shelves'])) & 
                       SeriesRaw.str.contains('|'.join(['wall','mtd','mount','detention'])),
                       'shelf detention',ReturnSeries)

    ReturnSeries=np.where((SeriesRaw.str.contains('|'.join(['occupancy']))==True),'occupancy sensor',ReturnSeries)
    ReturnSeries=np.where((SeriesRaw.str.contains('|'.join(['fan']))==True),'fan',ReturnSeries)

    ReturnSeries=np.where((SeriesRaw.str.contains('dental') & SeriesRaw.str.contains('chair')),
                       'dental chair',
                       ReturnSeries)
    
    
    
    ReturnSeries=np.where((SeriesRaw.str.contains('task') & SeriesRaw.str.contains('chair')),
                       'task chair',
                       ReturnSeries)


    ReturnSeries=np.where((SeriesRaw.str.contains('|'.join(['partion patient']))==True),'partion patient',ReturnSeries)

    ReturnSeries=np.where((SeriesRaw.str.contains('stack') & SeriesRaw.str.contains('chair')),
                       'stacking chair',
                       ReturnSeries)



    ReturnSeries=np.where((SeriesRaw.str.contains('|'.join(['wheel chair']))==True),'wheel chair',ReturnSeries)
    ReturnSeries=np.where((SeriesRaw.str.contains('|'.join(['locker']))==True),'locker',ReturnSeries)
    ReturnSeries=np.where((SeriesRaw.str.contains('|'.join(['sprinkler']))==True),'sprinkler',ReturnSeries)
    ReturnSeries=np.where((SeriesRaw.str.contains('|'.join(['heater','heating']))==True),'heater',ReturnSeries)
    ReturnSeries=np.where((SeriesRaw.str.contains('|'.join(['card']))==True),'card access reader',ReturnSeries)
    ReturnSeries=np.where((SeriesRaw.str.contains('|'.join(['speaker']))==True) & 
             (SeriesRaw.str.contains('|'.join(['fire']))==False),'speaker',ReturnSeries)
    
    ReturnSeries=np.where((SeriesRaw.str.contains('|'.join(['fire']))==True) & 
             (SeriesRaw.str.contains('|'.join(['alarm','sensor','speaker','strobe','detect']))==False),'fire alarm',ReturnSeries)
    
    ReturnSeries=np.where((SeriesRaw.str.contains('|'.join(['smoke']))==True),'smoke detector',ReturnSeries)
    
    ReturnSeries=np.where(SeriesRaw.str.contains('|'.join(['bunk','bed'])) & 
                       SeriesRaw.str.contains('|'.join(['wall','mtd','mount','detention'])),
                       'bed bunk detention',
                       ReturnSeries)


    ReturnSeries=np.where((SeriesRaw.str.contains('|'.join(['video']))==True) & 
             (SeriesRaw.str.contains('|'.join(['phone','vis']))==True),'video visitation',ReturnSeries)


    ReturnSeries=np.where((SeriesRaw.str.contains('|'.join(['fire']))==True) & 
             (SeriesRaw.str.contains('|'.join(['ext']))==True),'fire extinguisher',ReturnSeries)
    
    ReturnSeries=np.where((SeriesRaw.str.contains('|'.join(['eye was','eyewash']))==True) & 
             (SeriesRaw.str.contains('|'.join(['microphone']))==False),'eye wash',ReturnSeries)
    
    return ReturnSeries
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