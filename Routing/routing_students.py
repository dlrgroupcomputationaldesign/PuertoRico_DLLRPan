import requests
import pandas as pd

### CHANGE THESE ###

# spreadsheet with school coordinates (may need to update column names to match)
school_excel = "SchoolAssessmentData_06212022.xlsx"

# spreadsheet with student coordinates (may need to update column names to match)
student_excel = "student_addr_sample_PONCE_20220630.xls"

# municipality where students and schools are located
municipality = "YAUCO"

####################

# Functions

def get_route(start, end):
    """
    Get route from start to end coordinates

    Query the OpenRouteService API for a car route between the start
    and end coordinates.
    """
    url = f"http://localhost:8080/ors/v2/directions/driving-car?start={start}&end={end}"
    response = requests.get(url)
    return response

def get_school_df():
    """
    Load school data into pandas dataframe
    """
    df = pd.read_excel(school_excel, skiprows=1)
    return df

def make_coord(longitude, latitude):
    """
    Make a long,lat coordinate for the ORS API
    """
    return f'{longitude},{latitude}'

def get_school_coords(municipality):
    """
    Get the coordinates for each school in a municipality
    """
    df = get_school_df()
    municipality_df = df.loc[df['MUNICIPALITY'].str.lower() == municipality.lower()]
    schools = []
    for index, row in municipality_df.iterrows():
        latitude = row['LATITUDE']
        longitude = row['LONGITUDE']
        code = row['CODE']
        schools.append({
            'code': code,
            'longitude': longitude,
            'latitude': latitude
        })
    return schools

def get_student_df(municipality):
    """
    Load school data into pandas dataframe
    """
    students = pd.read_excel(student_excel)
    municipality_students = students.loc[students['Subregion'].str.lower() == municipality.lower()].copy()
    municipality_students['best_address'] = (municipality_students['Addr_type'].isin(['PointAddress', 'StreetAddress']))
    municipality_students = municipality_students.reset_index(drop=True)
    return municipality_students

def get_student_coords(municipality):
    """
    Get the student coordinates for each student in a municipality
    """
    municipality_students = get_student_df(municipality)
    students = []
    for index, row in municipality_students.iterrows():
        longitude = row['X']
        latitude = row['Y']
        best_address = row['best_address']
        current_school = row['Código_Escuela']
        students.append({
            'id': index,
            'longitude': longitude,
            'latitude': latitude,
            'best_address': best_address,
            'current_school': current_school
        })
    return students

# route students to schools in a municipality

schools = get_school_coords(municipality)
students = get_student_coords(municipality)

output = {
    'student': [],
    'school': [],
    'distance': [],
    'duration': [],
    'best_address': [],
    'current_school': []
}

for student in students:
    best_address = student['best_address']
    current_school = student['current_school']
    for school in schools:
        start = make_coord(student['longitude'], student['latitude'])
        end = make_coord(school['longitude'], school['latitude'])
        response = get_route(start, end)
        route = response.json()
        output['student'].append(student['id'])
        output['school'].append(school['code'])
        output['distance'].append(route['features'][0]['properties']['summary']['distance'])
        output['duration'].append(route['features'][0]['properties']['summary']['duration'])
        output['best_address'].append(best_address)
        output['current_school'].append(current_school)

# Data to dataframe

output_df = pd.DataFrame(output)
students_in_municipality = output_df.loc[output_df['school']==output_df['current_school'], 'student'].tolist()
output_df = output_df.loc[output_df['student'].isin(students_in_municipality)] # keep students going to school in the municipality

# Base case, no schools closed

# distance meters -> miles, duration seconds -> minutes
def get_stats(df):
    return {
        'mean_distance': df['distance'].mean() * 0.000621371,
        'max_distance': df['distance'].max() * 0.000621371,
        'mean_duration': df['duration'].mean() / 60,
        'max_duration': df['duration'].max() / 60
    }

no_closures = output_df.loc[output_df['school']==output_df['current_school']]
get_stats(no_closures)

# Close school
# Iterate through each school closing and move students whose school has closed to closest school
# TODO add assumptions about what schools students will move to
# Currently limited to only schools where a student in the sample is currently attending
# school_codes = [school['code'] for school in schools]

potential_codes = output_df['current_school'].unique().tolist()

def close_school(code, potential_codes):
    filtered_df = output_df.loc[output_df['current_school'].isin(potential_codes)].copy()
    # separate moving and staying students
    moving_students = filtered_df.loc[filtered_df['current_school']==code, 'student'].tolist()
    staying_students_df = filtered_df.loc[(filtered_df['school']==filtered_df['current_school']) & ~(filtered_df['student'].isin(moving_students))]
    moving_students_df = filtered_df.loc[(filtered_df['student'].isin(moving_students)) & (filtered_df['current_school']!=code)]
    # find closest school 
    moving_students_df = moving_students_df.loc[moving_students_df.groupby('student').distance.idxmin()]
    return_df = pd.concat([staying_students_df, moving_students_df]).sort_values('student')
    return return_df

# Iterate through potential school closures

for code in potential_codes:
    new_routing = close_school(code, potential_codes)
    print(code)
    print(get_stats(new_routing))

