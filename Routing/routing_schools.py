import requests
import json
import pandas as pd

### CHANGE THESE ###

# spreadsheet with school coordinates (may need to update column names to match)
school_excel = "SchoolAssessmentData_06212022.xlsx"

# region to route
region = "PONCE"

# should we save a csv of distance calculations for each combination of schools (distances)
# or
# a geojson of the routing directions between each comnbination of schools (routing)
distance_or_routing = "distance"

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

def get_school_coords(region):
    """
    Get the coordinates for every school in a region
    """
    df = get_school_df()
    region = df.loc[df['REGION'].str.lower() == region.lower()]
    schools = []
    for _, row in region.iterrows():
        latitude = row['LATITUDE']
        longitude = row['LONGITUDE']
        code = row['CODE']
        schools.append({
            'code': code,
            'longitude': longitude,
            'latitude': latitude
        })
    return schools

def save_json(file, filename):
    """
    Save an object as a json file
    """
    with open(filename, 'w') as f:
        json.dump(file, f, ensure_ascii=False, indent=4)

def route_to_feature(school_A, school_B, coordinates):
    """
    Format data for geojson feature
    """
    return {
        'type': 'Feature',
        'geometry': {
            'type': 'LineString',
            'coordinates': coordinates
        },
        'properties': {
            'start_school': school_A,
            'end_school': school_B
        }
    }


schools = get_school_coords(region)

if distance_or_routing == "distance":
    results = {
        'school_A': [],
        'school_B': [],
        'distance': [],
        'duration': []
    }
    # iterate through school combinations
    for school_A in schools:
        for school_B in schools:
            start = make_coord(school_A['longitude'], school_A['latitude'])
            end = make_coord(school_B['longitude'], school_B['latitude'])
            response = get_route(start, end)
            route = response.json()
            summary = route['features'][0]['properties']['summary']
            if len(summary.keys())==0: # capture zero distances
                results['distance'].append(0)
                results['duration'].append(0)
            else:
                results['distance'].append(route['features'][0]['properties']['summary']['distance'])
                results['duration'].append(route['features'][0]['properties']['summary']['duration'])
            results['school_A'].append(school_A['code'])
            results['school_B'].append(school_B['code'])
    # transform to a dataframe
    results_df = pd.DataFrame.from_dict(results)
    # save results as csv
    results_df.to_csv(f"{region}_routing.pkl", index=False)
else:
    output = {
        'type': 'FeatureCollection',
        'features': []
    }
    # iterate through school combinations
    for school_A in schools:
        for school_B in schools:
            if school_A != school_B:
                start = make_coord(school_A['longitude'], school_A['latitude'])
                end = make_coord(school_B['longitude'], school_B['latitude'])
                response = get_route(start, end)
                route = response.json()
                coordinates = route['features'][0]['geometry']['coordinates']
                feature = route_to_feature(school_A, school_B, coordinates)
                output['features'].append(feature)
    # save results as geojson
    save_json(output, f"{region}_routing.geojson")

