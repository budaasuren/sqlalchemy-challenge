
import sqlalchemy
from sqlalchemy.ext.automap import automap_base
from sqlalchemy.orm import Session
from sqlalchemy import create_engine, func
import datetime as dt
from flask import Flask, jsonify
import numpy as np

#################################################
# Database Setup
#################################################
engine = create_engine("sqlite:///Resources/hawaii.sqlite")

# reflect an existing database into a new model
Base = automap_base()
# reflect the tables
Base.prepare(autoload_with=engine)

# Save reference to the table
Measurement = Base.classes.measurement
Station = Base.classes.station

def date_formatter(date_to_format):
    to_list = date_to_format.split('-')
    formatted_date = dt.date(int(to_list[0]),int(to_list[1]),int(to_list[2]))
    return formatted_date

#################################################
# Flask Setup
#################################################
app = Flask(__name__)

@app.route("/")
def home_page():
    """List all available api routes."""
    return (
        f"Available Routes:<br/>"
        f"/api/v1.0/precipitation<br/>"
        f"/api/v1.0/stations<br/>"
        f"/api/v1.0/tobs<br/>"
        f"/api/v1.0/start/<start><br/>"
        f"/api/v1.0/start&end/<start>/<end><br/>"
    )

@app.route("/api/v1.0/precipitation")
def precipitation():
    # Create our session (link) from Python to the DB
    session = Session(engine)

    """Convert the query results from your precipitation analysis"""
    # Query to retrieve only the last 12 months of data for precipitation
    results = session.query(Measurement.date, Measurement.prcp).\
        filter(Measurement.date >= dt.date(2016, 8, 23)).all()

    session.close()

    # Create a dictionary from the row data and append to a list of prcp_analysis
    prcp_analysis = []
    for i in range(len(results)):
        prcp_analysis.append({results[i][0]:results[i][1]})

    return jsonify(prcp_analysis)
    
@app.route("/api/v1.0/stations")
def station():
    # Create our session (link) from Python to the DB
    session = Session(engine)

    '''Return a JSON list of stations from the dataset.'''
    results=session.query(Measurement.station).\
    distinct().all()

    session.close()

    # Convert list of tuples into normal list
    all_stations = list(np.ravel(results))

    return jsonify(all_stations)

@app.route("/api/v1.0/tobs")
def tobs():
    # Create our session (link) from Python to the DB
    session = Session(engine)

    #Query the dates and temperature observations of the most-active station

    results=session.query(Measurement.date, Measurement.tobs).\
        filter(Measurement.date >= dt.date(2016, 8, 23)).\
        filter(Measurement.station=='USC00519281').all()
    
    session.close()
    #Return a JSON list of temperature observations for the previous year
    #temp_list = [ a[1] for a in results ]
    #return jsonify(temp_list)
    temp_list=[]
    for date, tobs in results:
        temp_dict={}
        temp_dict['date']=date
        temp_dict['tobs']=tobs
        temp_list.append(temp_dict)

    return jsonify(temp_list)

@app.route("/api/v1.0/start/<start>")
def query_start(start):
    # Create our session (link) from Python to the DB
    session = Session(engine)

    """Fetch list of the minimum temperature, the average temperature, andthe 
    maximum temperature for a specified start or start-end range, or a 404 if not."""

    sel = [Measurement.date, 
       func.avg(Measurement.tobs), 
       func.max(Measurement.tobs), 
       func.min(Measurement.tobs) 
       ]
    results = session.query(*sel).\
    group_by(Measurement.date).all()

    session.close()

    canonicalized = date_formatter(start)
    
    match = False
    temp_agg_list=[]
    for i in range(len(results)):
        search_term = date_formatter(results[i][0])
        if search_term >= canonicalized:
            match = True
            temp_agg_list.append({'date':results[i][0],\
                                  'avg temp':results[i][1],\
                                  'max temp':results[i][2],\
                                  'min temp':results[i][3]})
    if match is True:
        return jsonify(temp_agg_list)
    else:
        return jsonify({"error": f"data for {start} not found."}), 404


@app.route("/api/v1.0/start&end/<start>/<end>")
def query_start_end(start, end):
    # Create our session (link) from Python to the DB
    session = Session(engine)

    """Fetch list of the minimum temperature, the average temperature, andthe 
    maximum temperature for a specified start or start-end range, or a 404 if not."""

    sel = [Measurement.date, 
       func.avg(Measurement.tobs), 
       func.max(Measurement.tobs), 
       func.min(Measurement.tobs) 
       ]
    results = session.query(*sel).\
    group_by(Measurement.date).all()

    session.close()

    start_canonicalized = date_formatter(start)
    end_canonicalized = date_formatter(end) 

    match = False
    temp_agg_list=[]
    for i in range(len(results)):
        search_term = date_formatter(results[i][0])
        if ( search_term >= start_canonicalized ) and ( search_term <= end_canonicalized ) :
            match = True
            temp_agg_list.append({'date':results[i][0],\
                                  'avg temp':results[i][1],\
                                  'max temp':results[i][2],\
                                  'min temp':results[i][3]})
    if match is True:
        return jsonify(temp_agg_list)
    else:
        return jsonify({"error": f"data for {start} not found."}), 404

if __name__ == '__main__':
    app.run(debug=True)


