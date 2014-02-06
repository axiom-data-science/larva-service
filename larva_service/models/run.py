from mongokit import Document, DocumentMigration
from larva_service import db, app, redis_connection
from flask import url_for
from datetime import datetime
from dateutil.parser import parse as dateparse
import json
import urllib2
import pytz
import os
import calendar
from urlparse import urlparse
import mimetypes
from shapely.geometry import Point, Polygon, asShape
import geojson
from shapely.wkt import loads
from rq.job import Job

class RunMigration(DocumentMigration):
    def allmigration01__add_results_field(self):
        self.target = {'task_result':{'$exists': False}}
        self.update = {'$set':{'task_result':""}}

    def allmigration02__add_name_field(self):
        self.target = {'name':{'$exists': False}}
        self.update = {'$set':{'name':""}}

    def allmigration03__add_duration_field(self):
        self.target = {'ended':{'$exists': False}}
        self.update = {'$set':{'ended':""}}

    def allmigration04__add_shoreline_fields(self):
        self.target = {'shoreline_path':{'$exists':False}, 'shoreline_feature':{'$exists':False}}
        self.update = {'$set':{'shoreline_path':u'', 'shoreline_feature':u''}}

    def allmigration05__add_caching_field(self):
        self.target = {'caching' : {'$exists' : False }}
        self.update = {'$set' : {'caching' : True }}

class Run(Document):
    __collection__ = 'runs'
    use_dot_notation = True
    structure = {
       'name'               : unicode,
       'behavior'           : unicode,  # URL to Behavior JSON
       'cached_behavior'    : dict,     # Save the contents of behavior URL
       'particles'          : int,      # Number of particles to force
       'hydro_path'         : unicode,  # OPeNDAP or Local file path
       'geometry'           : unicode,  # WKT
       'release_depth'      : float,    # Release depth
       'start'              : datetime, # Release in time
       'duration'           : int,      # Days
       'timestep'           : int,      # In seconds, the timestep between calculations
       'horiz_dispersion'   : float,    # Horizontal dispersion, in m/s
       'vert_dispersion'    : float,    # Horizontal dispersion, in m/s
       'time_chunk'         : int,
       'horiz_chunk'        : int,
       'time_method'        : unicode,  # Time method, 'nearest' or 'interp'
       'created'            : datetime,
       'task_id'            : unicode,
       'email'              : unicode,   # Email of the person who ran the model
       'output'             : list,
       'task_result'        : unicode,
       'trackline'          : unicode,
       'ended'              : datetime,
       'shoreline_path'     : unicode,
       'shoreline_feature'  : unicode,
       'caching'            : bool
    }
    default_values = {
                      'created': datetime.utcnow,
                      'time_chunk'  : 10,
                      'horiz_chunk' : 5,
                      'time_method' : u'interp',
                      'caching'     : True
                      }
    migration_handler = RunMigration

    restrict_loading = ["output", "task_result", "trackline", "task_id", "created", "cached_behavior","output", "ended"]

    def compute(self):
        """
        Add any metadata to this object from the model run output
        """
        try:
            self.set_trackline()
        except:
            app.logger.warning("Could not process trackline results.  URL may be invalid?")

        if Job.exists(self.task_id, connection=redis_connection):
            job = Job.fetch(self.task_id, connection=redis_connection)
            self.task_result = unicode(job.meta.get("outcome", ""))

        self.save()

    def set_trackline(self):
        if self.trackline is None:
            for filepath in self.output:
                if os.path.splitext(os.path.basename(filepath))[1] == ".geojson":
                    # Get GeoJSON trackline and cache locally as WKT
                    t = urllib2.urlopen(filepath)
                    self.trackline = unicode(asShape(geojson.loads(t.read())).wkt)
        return self.trackline

    def status(self):
        if self.task_result is not None and self.task_result != "":
            return self.task_result
        elif Job.exists(self.task_id, connection=redis_connection):
            job = Job.fetch(self.task_id, connection=redis_connection)
            return job.status
        else:
            return "unknown"

    def progress(self):
        if self.task_result is not None and self.task_result != "":
            return 100
        elif Job.exists(self.task_id, connection=redis_connection):
            job = Job.fetch(self.task_id, connection=redis_connection)
            return job.meta.get("progress", 0)
        else:
            return "unknown"

    def message(self):
        if self.task_result is not None and self.task_result != "":
            return self.task_result
        elif Job.exists(self.task_id, connection=redis_connection):
            job = Job.fetch(self.task_id, connection=redis_connection)
            return job.meta.get("message", None)
        else:
            return "unknown"

    def last_progress_update(self):
        if self.task_result is not None and self.task_result != "":
            return "run complete"
        elif Job.exists(self.task_id, connection=redis_connection):
            job = Job.fetch(self.task_id, connection=redis_connection)
            return job.meta.get("updated", None)
        else:
            return "unknown"

    def google_maps_trackline(self):
        if self.trackline:
            geo = loads(self.trackline)
            return geo.coords

        return []

    def google_maps_coordinates(self):
        marker_positions = []
        if self.geometry:
            geo = loads(self.geometry)
            # Always make a polygon
            if isinstance(geo, Point):
                marker_positions.append((geo.coords[0][1], geo.coords[0][0]))
            else:
                for pt in geo.exterior.coords:
                    # Google maps is y,x not x,y
                    marker_positions.append((pt[1], pt[0]))

        return marker_positions

    def get_file_key_and_link(self, file_path):
        url = urlparse(file_path)
        if url.scheme == '':
            base_access_url = app.config.get('NON_S3_OUTPUT_URL', None)
            if base_access_url is None:
                # Serve assets through Flask... not desired!
                app.logger.warning("Serving static output through Flask is not desirable! Set the NON_S3_OUTPUT_URL config variable.")

                filename = url.path.split("/")[-1]
                # We can get a context here to use url_for
                with app.app_context():
                    file_link = url_for("run_output_download", run_id=self._id, filename=filename)
            else:
                file_link = base_access_url + file_path.replace(app.config.get("OUTPUT_PATH"), "")

        else:
            file_link = file_path

            # Local file
        url = urlparse(file_link)
        name, ext = os.path.splitext(url.path)

        file_type = "Unknown (%s)" % ext
        if ext == ".zip":
            file_type = "Shapefile"
        elif ext == ".nc":
            file_type = "NetCDF"
        elif ext == ".cache":
            file_type = "Forcing Data"
        elif ext == ".json":
            file_type = "JSON"
        elif ext == ".geojson":
            file_type = "Trackline (GeoJSON)"
        elif ext == ".avi":
            file_type = "Animation"
        elif ext == ".log":
            file_type = "Logfile"

        return { file_type : file_link }

    def output_files(self):
        return (self.get_file_key_and_link(file_path) for file_path in self.output)

    def run_config(self):

        skip_keys = ['_id', 'cached_behavior', 'created', 'task_id', 'output', 'trackline', 'task_result', 'ended']
        d = {}
        for key, value in self.iteritems():
            if key not in skip_keys:
                if key == 'start':
                    d[key] = value.isoformat()
                else:
                    d[key] = value

        return d

    def load_run_config(self, run):
        # Set the 1:1 relationship between the config and this object
        for key, value in run.iteritems():

            # Don't save keys that shouldn't be part of an initial run object
            if key in Run.restrict_loading:
                continue

            if key == 'start':
                # Text DateTime
                try:
                    # Convert to UTC
                    d = dateparse(value)
                    if d.tzinfo is None:
                        d = d.replace(tzinfo=pytz.utc)
                    self[key] = d.astimezone(pytz.utc)
                except:
                     # Timestamp DateTime  (assume in UTC)
                    try:
                        self[key] = datetime.fromtimestamp(value / 1000, pytz.utc)
                    except:
                        raise

            elif key == 'release_depth' or key == 'horiz_dispersion' or key == 'vert_dispersion':
                self[key] = float(value)

            else:
                self[key] = value

        if self.behavior:
            try:
                b = urllib2.urlopen(self.behavior)
                self.cached_behavior = json.loads(b.read())
            except:
                pass


db.register([Run])


from tables import *
# Pytables representation of a model run
class ModelResultsTable(IsDescription):
    particle    = UInt8Col()
    time        = Time32Col()
    latitude    = Float32Col()
    longitude   = Float32Col()
    depth       = Float32Col()
    u_vector    = Float32Col()
    v_vector    = Float32Col()
    w_vector    = Float32Col()
    temperature = Float32Col()
    salinity    = Float32Col()
    age         = Float32Col()
    lifestage   = UInt8Col()
    progress    = Float32Col()
    settled     = BoolCol()
    halted      = BoolCol()
    dead        = BoolCol()


class ResultsPyTable(object):
    def __init__(self, output_file):
        self._file  = open_file(output_file, mode="w", title="Model run output")
        self._root  = self._file.create_group("/", "trajectories", "Trajectory Data")
        self._table = self._file.create_table(self._root, "model_results", ModelResultsTable, "Model Results")

    def write(self, data):
        record = self._table.row
        for k, v in data.iteritems():
            try:
                record[k] = v
            except Exception, e:
                raise
                # No column named "k", so don't add the data
                pass

        record.append()

    def trackline(self):
        pass

    def metadata(self):
        pass

    def compute(self):
        self.trackline()
        self.metadata()

    def close(self):
        self._table.flush()
        self._file.close()