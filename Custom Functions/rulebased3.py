import numpy as np
import pandas as pd 
import numpy as np

def ruleclassifier(SeriesRaw: pd.Series,SeriesLabs=False):
    if len(SeriesLabs)==1:
        SeriesLabs=SeriesRaw.copy()
    
    ReturnSeries=np.where((SeriesRaw.str.contains('|'.join(['phone']))==True) & 
             (SeriesRaw.str.contains('|'.join(['microphone']))==False),'phone',SeriesLabs)
    
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
    ReturnSeries=np.where((SeriesRaw.str.contains('|'.join(['damper']))==True),'damper',ReturnSeries)
    ReturnSeries=np.where((SeriesRaw.str.contains('|'.join(['mullion']))==True),'mullion',ReturnSeries)


    #note these ones
    ReturnSeries=np.where((SeriesRaw.str.contains('|'.join(['fence']))==True),'fence',ReturnSeries)
    ReturnSeries=np.where((SeriesRaw.str.contains('|'.join(['gate']))==True),'gate',ReturnSeries)
    ReturnSeries=np.where((SeriesRaw.str.contains('|'.join(['television']))==True),'television',ReturnSeries)

    ReturnSeries=np.where((SeriesRaw.str.contains('|'.join(['air condition']))==True),'air conditioner',ReturnSeries)
    ReturnSeries=np.where((SeriesRaw.str.contains('|'.join(['coffee maker']))==True),'coffee maker',ReturnSeries)
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