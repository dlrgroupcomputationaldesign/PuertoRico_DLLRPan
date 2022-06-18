#Gets areas from DLRBIMSQL models 
import pandas as pd
import pyodbc 

def get_areas(): #meh
    
    
    q="""
    SELECT [UniqueId],[Area]
    FROM [ClarityIndex].[dbo].[PQ_Rooms] 
    group by  [UniqueId],[Area]
    """
    conn = pyodbc.connect('Driver={SQL Server};'
                      'Server=DLRBIMSQL;'
                      'Database=ClarityIndex;'
                      'Trusted_Connection=yes;')
    cursor = conn.cursor()
    
    data=pd.read_sql_query(q,conn)
    
    cursor.close()
    conn.close()
    
    
    return data


def get_courts_rooms(): #ehh
    
    q = """ 
    SELECT  PQRC.RoomModelId,
            PROJ.ProjectName,
            PQRC.RoomId,
            RoomName,
            'Court' as BuildingType
    FROM [ClarityIndex].[dbo].[PQ_RoomContents] PQRC
    left join (SELECT PQPI.ProjectName,PQPI.ModelRefId
    FROM [ClarityIndex].[dbo].[PQ_ProjectInformation] PQPI
    where (
     PQPI.ProjectName like '%justice%' or PQPI.ProjectName like '%state court%' or 
     PQPI.ProjectName like '%federal court%' or PQPI.ProjectName like '%state court%' or
     PQPI.ProjectName like '%courthouse%' or PQPI.ProjectName like '%court %' or
     PQPI.ProjectName like '%courts%' or PQPI.ProjectName like '%muncipal court%' or
     PQPI.ProjectName like '%jury%' or PQPI.ProjectName like '%judge%' or
     PQPI.ProjectName like '%judical%' or PQPI.ProjectName like '%county court%' or
     PQPI.ProjectName like '%court house%'
        )
    group by PQPI.ModelRefId,PQPI.ProjectName
    ) PROJ
    on PQRC.RoomModelId=PROJ.ModelRefId
    left join (
    select BASE.RoomModelId,count(*) as total 
    from  
    (select PQRC.RoomModelId,PQRC.RoomId,PQRC.RoomName 
    FROM [ClarityIndex].[dbo].[PQ_RoomContents] PQRC
    group by PQRC.RoomModelId,PQRC.RoomId,PQRC.RoomName) BASE
    WHERE 
    ---People
        (RoomName LIKE '%LAW CLERK%' or RoomName LIKE '%JUDGE%' or RoomName LIKE '%JUDICIAL%' or RoomName LIKE '%JURY%' or
        RoomName LIKE '%COURT%') and 
        RoomName NOT LIKE '%courtyard%' and RoomName NOT LIKE '%BASKETBALL%' and 
        RoomName NOT LIKE '%volleyball%' and RoomName NOT LIKE '%food court%' 
    group by 
        BASE.RoomModelId
    having count(*)>=4
    ) ducks
    on PQRC.RoomModelId=ducks.RoomModelId
    --15970
    where 
    --Room Names Look Like Police Station
    ducks.RoomModelId is not null or 
    --Project Name Looks Like Police Station
    PROJ.ModelRefId is not null or 
    --Model Name Looks Like Police Station
    PQRC.RoomModelName like '%justice%' or PQRC.RoomModelName like '%state court%' or 
     PQRC.RoomModelName like '%federal court%' or PQRC.RoomModelName like '%state court%' or
     PQRC.RoomModelName like '%courthouse%' or PQRC.RoomModelName like '%court %' or
     PQRC.RoomModelName like '%courts%' or PQRC.RoomModelName like '%muncipal court%' or
     PQRC.RoomModelName like '%jury%' or PQRC.RoomModelName like '%judge%' or
     PQRC.RoomModelName like '%judical%' or PQRC.RoomModelName like '%county court%' or
     PQRC.RoomModelName like '%court house%'
    group by PQRC.RoomId,RoomName,PQRC.RoomModelId,PROJ.ProjectName
    """
    conn = pyodbc.connect('Driver={SQL Server};'
                      'Server=DLRBIMSQL;'
                      'Database=ClarityIndex;'
                      'Trusted_Connection=yes;')
    cursor = conn.cursor()
    
    data=pd.read_sql_query(q,conn)
    
    cursor.close()
    conn.close()
    
    
    return data
    
    
def get_detention_rooms(): #ehh

    q = """ 
SELECT  PQRC.RoomModelId,
		PROJ.ProjectName,
		PQRC.RoomId,
		RoomName,
        'Detention Center' as BuildingType
FROM [ClarityIndex].[dbo].[PQ_RoomContents] PQRC
left join (SELECT PQPI.ProjectName,PQPI.ModelRefId
FROM [ClarityIndex].[dbo].[PQ_ProjectInformation] PQPI
where (
 PQPI.ProjectName like '%jail%'
or PQPI.ProjectName like '%prision%'
or PQPI.ProjectName like '%correction%'
or PQPI.ProjectName like '%detention%'
or PQPI.ProjectName like '%juve%'
or PQPI.ProjectName like '%confin%'
or PQPI.ProjectName like '%security housing%'
)
group by PQPI.ModelRefId,PQPI.ProjectName

) PROJ
on PQRC.RoomModelId=PROJ.ModelRefId
left join (

select BASE.RoomModelId,count(*) as total from  
(select PQRC.RoomModelId,PQRC.RoomId,PQRC.RoomName 
FROM [ClarityIndex].[dbo].[PQ_RoomContents] PQRC
group by PQRC.RoomModelId,PQRC.RoomId,PQRC.RoomName) BASE
WHERE RoomName LIKE '% cell' or RoomName LIKE '% cells' 
	or RoomName LIKE '%medium security%' 
	or RoomName LIKE '%high security%'
	or RoomName LIKE '%officer%'
	or RoomName LIKE '%max security%' 
	or RoomName LIKE '%sally %'
	or RoomName LIKE '% sally %'
	or RoomName LIKE '%inmate%' 
	or RoomName LIKE '%sallyport%' 
	or RoomName LIKE '%kennel%' 
	or RoomName LIKE '%segregation%'
	or RoomName='CELL'
group by 
	BASE.RoomModelId
having count(*)>=9

) ducks
on PQRC.RoomModelId=ducks.RoomModelId

where ducks.RoomModelId is not null or PROJ.ModelRefId is not null or PQRC.RoomModelName LIKE '%Jail%'
or PQRC.RoomModelName LIKE '%prision%' or PQRC.RoomModelName LIKE '%correction%' or PQRC.RoomModelName LIKE '%detention%'

group by PQRC.RoomId,
	RoomName,
	PQRC.RoomModelId,
	PROJ.ProjectName
"""

    conn = pyodbc.connect('Driver={SQL Server};'
                      'Server=DLRBIMSQL;'
                      'Database=ClarityIndex;'
                      'Trusted_Connection=yes;')
    cursor = conn.cursor()
    
    data=pd.read_sql_query(q,conn)
    
    cursor.close()
    conn.close()
    
    
    return data

def get_police_rooms(): #ehh

    q = """
SELECT  PQRC.RoomModelId,
		PROJ.ProjectName,
		PQRC.RoomId,
		RoomName,
        'Law Enforcement' as BuildingType
FROM [ClarityIndex].[dbo].[PQ_RoomContents] PQRC
left join (SELECT PQPI.ProjectName,PQPI.ModelRefId
FROM [ClarityIndex].[dbo].[PQ_ProjectInformation] PQPI
where (
 PQPI.ProjectName like '%police%' or PQPI.ProjectName like '%sheriff%' or 
 PQPI.ProjectName like '%law enforce%' or PQPI.ProjectName like '%squad%' or 
 PQPI.ProjectName like '%public safety%'
)
group by PQPI.ModelRefId,PQPI.ProjectName
) PROJ
on PQRC.RoomModelId=PROJ.ModelRefId
left join (
select BASE.RoomModelId,count(*) as total 
from  
(select PQRC.RoomModelId,PQRC.RoomId,PQRC.RoomName 
FROM [ClarityIndex].[dbo].[PQ_RoomContents] PQRC
group by PQRC.RoomModelId,PQRC.RoomId,PQRC.RoomName) BASE
WHERE 
---People
	RoomName LIKE '%detective%' or RoomName LIKE '%sheriff%' or RoomName LIKE '%police%' or RoomName LIKE '%deputy%' or
	RoomName LIKE '%captain%' or RoomName LIKE '%sergeant%' or RoomName LIKE '%civilian%' or RoomName LIKE '%cadet%' or
    RoomName LIKE '%LIEUTENANT%' or RoomName LIKE '%NCOIC%' or
---Things
	RoomName LIKE '%taser%' or RoomName LIKE '%fire arm%' or RoomName LIKE '%evidence%' or RoomName LIKE '%firearm%' or 
	RoomName LIKE '%polygraph%' or RoomName LIKE '%marijuana%' or
---Functions
	RoomName LIKE '%crime lab%' or RoomName LIKE '%finger print%' or RoomName LIKE '%fingerprint%' or RoomName LIKE '%SWAT%' or 
	RoomName LIKE '%patrol%' or RoomName LIKE '%Graffiti%' or RoomName LIKE '%street crime%' or RoomName LIKE '%gang%' or RoomName LIKE '%DARE%' or
    RoomName LIKE '%HUMAN TRAFFICKING%' or RoomName LIKE '%MISSION PLANNING%' or RoomName LIKE '%WIRE ROOM%'
group by 
	BASE.RoomModelId
having count(*)>=4
) ducks
on PQRC.RoomModelId=ducks.RoomModelId
--15970
where 
--Room Names Look Like Police Station
ducks.RoomModelId is not null or 
--Project Name Looks Like Police Station
PROJ.ModelRefId is not null or 
--Model Name Looks Like Police Station
PQRC.RoomModelName LIKE '%police%' or PQRC.RoomModelName LIKE '%sheriff%' or 
PQRC.RoomModelName LIKE '%law enforce%' or PQRC.RoomModelName LIKE '%squad%' or 
PQRC.RoomModelName like '%public safety%'
group by PQRC.RoomId,RoomName,PQRC.RoomModelId,PROJ.ProjectName
"""
    conn = pyodbc.connect('Driver={SQL Server};'
                      'Server=DLRBIMSQL;'
                      'Database=ClarityIndex;'
                      'Trusted_Connection=yes;')
    cursor = conn.cursor()
    
    data=pd.read_sql_query(q,conn)
    
    cursor.close()
    conn.close()
    
    
    return data

def get_jcother_rooms():
    q= """
select PQRC.RoomModelId,PQPI.ProjectName,PQRC.RoomId,PQRC.RoomName, 'Misc JC' as BuildingType
from dbo.PQ_RoomContents PQRC
left join (select PQPI.ProjectName,PQPI.projectId,PQPI.ModelRefId from dbo.PQ_ProjectInformation PQPI) PQPI
ON PQRC.RoomModelId=PQPI.ModelRefId

WHERE PQRC.projectid IN 
(106,	108,	89,	131,	431,	454,	468,	475,	482,	486,	521,	527,	139,	77,
	59,	99,	456,	562,	563,	564,	404,	126,	84,	60,	428,	565,	597,	598,	634,	
	693,	695,	710,	716,	718,	730,	798,	856,	857,	870,	182,	882,	93,	898,	104,	
	729,	184,	240,	427,	709,	649,	161,	1318,	1346,	1360) 

OR PQPI.projectId IN (106,	108,	89,	131,	431,	454,	468,	475,	482,	486,	521,	527,	139,	77,
	59,	99,	456,	562,	563,	564,	404,	126,	84,	60,	428,	565,	597,	598,	634,	
	693,	695,	710,	716,	718,	730,	798,	856,	857,	870,	182,	882,	93,	898,	104,	
	729,	184,	240,	427,	709,	649,	161,	1318,	1346,	1360)

OR ((PQPI.ProjectName like '%justice%' or PQPI.ProjectName like '%civic%' or PQPI.ProjectName like '%jail%' or PQPI.ProjectName like '%prison%'
	or PQPI.ProjectName like '%correction%' or PQPI.ProjectName like '%detention%' 
	or PQPI.ProjectName like '%court house%' or PQPI.ProjectName like '%courthouse%'
	or PQPI.ProjectName like '%confinement%' or PQPI.ProjectName like '%justice%' 
	or PQPI.ProjectName like '%security housing%' or PQPI.ProjectName like '%juve%') and(PQPI.ProjectName not like '%school%' and PQPI.ProjectName not like '%isd%' 
	and PQPI.ProjectName not like '%Academy%'))

OR ((PQRC.RoomModelName like '%justice%' or PQRC.RoomModelName like '%civic%' or PQRC.RoomModelName like '%jail%' or PQRC.RoomModelName like '%prison%'
	or PQRC.RoomModelName like '%correction%' or PQRC.RoomModelName like '%detention%' or PQRC.RoomModelName like '%court house%' or PQRC.RoomModelName like '%courthouse%'
	or PQRC.RoomModelName like '%confinement%' or PQRC.RoomModelName like '%justice%' or PQRC.RoomModelName like '%security housing%' 
	or PQRC.RoomModelName like '%juve%') and(PQRC.RoomModelName like '%school%' and PQRC.RoomModelName not like '%isd%' and PQRC.RoomModelName not like '%Academy%'))
group by PQRC.RoomModelId,PQPI.ProjectName,PQRC.RoomId,PQRC.RoomName
"""

    conn = pyodbc.connect('Driver={SQL Server};'
                      'Server=DLRBIMSQL;'
                      'Database=ClarityIndex;'
                      'Trusted_Connection=yes;')
    cursor = conn.cursor()
    
    data=pd.read_sql_query(q,conn)
    
    cursor.close()
    conn.close()    
    
    return data

def get_rc(RC):
    q="""
SELECT PQRC.RoomModelId,
        PQRC.RoomId,
        PQRC.RoomName,
        PQRC.RoomModelName,
        PQRC.ItemId,
        PQRC.Category,
        PQRC.[Name]
FROM [ClarityIndex].[dbo].[PQ_RoomContents] PQRC
WHERE  PQRC.RoomModelId  in """ + \
    str(list(RC.drop_duplicates(subset=['RoomModelId'])['RoomModelId'])).replace('[','(').replace(']',')') + \
'group by PQRC.RoomModelId,PQRC.RoomId,PQRC.ItemId,PQRC.[Name],PQRC.RoomName,PQRC.RoomModelName,PQRC.Category'
    
    conn = pyodbc.connect('Driver={SQL Server};'
                      'Server=DLRBIMSQL;'
                      'Database=ClarityIndex;'
                      'Trusted_Connection=yes;')
    cursor = conn.cursor()
    
    RCFranken=pd.read_sql_query(q,conn)
    
    cursor.close()
    conn.close()
    
    return RC.set_index('RoomId').join(RCFranken.set_index('RoomId')[['ItemId','Category','Name']])

def get_manufact():
    q = """ 
SELECT Manufacturer,count(*) as totals
  FROM [ClarityIndex].[dbo].[PQ_RoomContents]
  where Manufacturer IS NOT NULL and Manufacturer<>' '
  GROUP BY Manufacturer
  Having count(*)>50
  order by count(*) DESC
"""
    conn = pyodbc.connect('Driver={SQL Server};'
                      'Server=DLRBIMSQL;'
                      'Database=ClarityIndex;'
                      'Trusted_Connection=yes;')
    cursor = conn.cursor()
    
    data=pd.read_sql_query(q,conn)
    
    cursor.close()
    conn.close()    
    
    return data