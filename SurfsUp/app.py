# Import the dependencies.
import numpy as np
import pandas as pd
import datetime as dt

import sqlalchemy
from sqlalchemy.ext.automap import automap_base
from sqlalchemy.orm import Session
from sqlalchemy import create_engine, func

from flask import Flask, jsonify
from dateutil.relativedelta import relativedelta

#################################################
# Database Setup
#################################################
# create engine to hawaii.sqlite
engine = create_engine("sqlite:///../Resources/hawaii.sqlite")

# reflect an existing database into a new model
Base = automap_base()

# reflect the tables
Base.prepare(engine, reflect=True)

# Save references to each table
measurement_table = Base.classes.measurement
station_table = Base.classes.station

# Create our session (link) from Python to the DB
session = Session(engine)

#################################################
# Flask Setup
#################################################

app = Flask(__name__)

#################################################
# Flask Routes
#################################################

# 1. Start at the homepage and list all the available routes.

@app.route("/")
def home():
    return(
        f"Welcome to the main page!<br/>"
        f"The available routes are:<br/>"
        f"/api/v1.0/precipitation<br/>"
        f"/api/v1.0/stations<br/>"
        f"/api/v1.0/tobs<br/>"
        f"/api/v1.0/start_date <br/>"
        f"/api/v1.0/start_date/end_date"
    )

# 2. Convert the query results from your precipitation analysis (i.e. retrieve only the last 12 months of data) to a dictionary using date as the key and prcp as the value.
#Return the JSON representation of your dictionary.

@app.route("/api/v1.0/precipitation")
def precipitation():
    session = Session(engine)

# Calculate the date one year ago from the last date in the database
    last_date = session.query(func.max(measurement_table.date)).scalar()
    one_year_ago = dt.datetime.strptime(last_date, "%Y-%m-%d") - relativedelta(months=12)

# Query precipitation data for the last 12 months
    selection = [measurement_table.date, measurement_table.prcp]
    precipitation_info = session.query(*selection).filter(measurement_table.date >= one_year_ago).all()
    session.close()

    precipitation_list = []
    date_list = []
    for date, prcp in precipitation_info:
        precipitation_list.append(prcp)
        date_list.append(date)
    ziped = zip(date_list, precipitation_list)
    precipitation_dict = dict(ziped)
    return jsonify(precipitation_dict)

# 3. Return a JSON list of stations from the dataset.

@app.route("/api/v1.0/stations")
def stats():
    print("/api/v1.0/stations")
    session = Session(engine)

    sel = [
        station_table.id,
        station_table.station,
        station_table.name,
        station_table.latitude,
        station_table.longitude,
        station_table.elevation
    ]
    results = session.query(*sel).all()
    session.close()
    listy = []
    for id, station, name, lat, lng, elev in results:
        dicty = {}
        dicty['id'] = id
        dicty['station'] = station
        dicty['name'] = name
        dicty['lat'] = lat
        dicty['lng'] = lng
        dicty['elevation'] = elev
        listy.append(dicty)
    return jsonify(listy) 


# 4. Query the dates and temperature observations of the most-active station for the previous year of data and return a JSON list of temperature observations for the previous year.

@app.route("/api/v1.0/tobs")
def temps():
    print("/api/v1.0/tobs")
    session = Session(engine)
    # We use this query to find the most active station
    active = session.query(measurement_table.station, func.count(measurement_table.station)).group_by(measurement_table.station).\
        order_by(func.count(measurement_table.station).desc()).first()
    # Then we grab all the tobs data from the last year at the most active station
    result = session.query(measurement_table.date, measurement_table.tobs).filter(measurement_table.date >= '2016-08-23').filter(measurement_table.station == active[0]).all()
    session.close()
    listy = []
    for date, temp in result:
        dicty = {}
        dicty[date] = temp
        listy.append(dicty)
    return jsonify(listy)

# 5. Return a JSON list of the minimum temperature, the average temperature, and the maximum temperature for a specified start or start-end range.
# For a specified start, calculate TMIN, TAVG, and TMAX for all the dates greater than or equal to the start date.
# For a specified start date and end date, calculate TMIN, TAVG, and TMAX for the dates from the start date to the end date, inclusive.

@app.route("/api/v1.0/<start>")
def starty(start):
    print(f"/api/v1.0/{start}")
    session = Session(engine)
    result = session.query(func.min(measurement_table.tobs), func.avg(measurement_table.tobs), func.max(measurement_table.tobs)).\
        filter(measurement_table.date >= start).all()
    # Here we grab the first and last dates in the dataset so we have edges
    min_date = session.query(measurement_table.date).order_by(measurement_table.date).first()
    max_date = session.query(measurement_table.date).order_by(measurement_table.date.desc()).first()
    session.close()
    
    # We use this if statement to make sure that the start date is within the dataset
    # if it's not then we return the valid dates to choose from
    if (start > max_date[0]) or (start < min_date[0]):
        return f"The start date you have input is not in the database, the earliest date is {min_date[0]} and the latest date is {max_date[0]}"
    
    listy = []
    for minnie, avg, maxy in result:
        dicty = {}
        dicty['min temp'] = minnie
        dicty['avg temp'] = avg
        dicty['max temp'] = maxy
        listy.append(dicty)
    return jsonify(listy)

# Then we have our second dynamic route
@app.route("/api/v1.0/<start>/<end>")
def endy(start, end):
    print(f"/api/v1.0/{start}/{end}")
    session = Session(engine)
    result = session.query(func.min(measurement_table.tobs), func.avg(measurement_table.tobs), func.max(measurement_table.tobs)).\
        filter(measurement_table.date >= start).filter(measurement_table.date <= end).all()
    min_date = session.query(measurement_table.date).order_by(measurement_table.date).first()
    max_date = session.query(measurement_table.date).order_by(measurement_table.date.desc()).first()
    session.close()

    if (start > max_date[0]) or (start < min_date[0]) or (end > max_date[0]) or (end < min_date[0]):
        return f"The start or end date you have input is not in the database, the earliest date is {min_date[0]} and the latest date is {max_date[0]}"

    listy = []
    for minnie, avg, maxy in result:
        dicty = {}
        dicty['min temp'] = minnie
        dicty['avg temp'] = avg
        dicty['max temp'] = maxy
        listy.append(dicty)
    return jsonify(listy)

# Finally we run the app 

if __name__ == "__main__":
    app.run(debug=True)