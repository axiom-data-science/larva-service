from mongokit import Document, DocumentMigration
from larva_service import db, app, redis_connection
from flask import url_for
from datetime import datetime, date
import json
import urllib2
import pytz
import os
from urlparse import urlparse
from shapely.geometry import Point, asShape
import geojson
from shapely.wkt import loads
from rq.job import Job


class RunMigration(DocumentMigration):
    def allmigration01__add_results_field(self):
        self.target = {'task_result': {'$exists': False}}
        self.update = {'$set': {'task_result': ""}}

    def allmigration02__add_name_field(self):
        self.target = {'name': {'$exists': False}}
        self.update = {'$set': {'name': ""}}

    def allmigration03__add_duration_field(self):
        self.target = {'ended': {'$exists': False}}
        self.update = {'$set': {'ended': ""}}

    def allmigration04__add_shoreline_fields(self):
        self.target = {'shoreline_path': {'$exists': False}, 'shoreline_feature': {'$exists': False}}
        self.update = {'$set': {'shoreline_path': u'', 'shoreline_feature': u''}}

    def allmigration05__add_started_fields(self):
        self.target = {'started': {'$exists': False}}
        self.update = {'$set': {'started': ''}}

    def allmigration06__add_final_message_fields(self):
        self.target = {'final_message': {'$exists': False}}
        self.update = {'$set': {'final_message': ''}}


class Run(Document):
    __collection__ = 'runs'
    use_dot_notation = True
    structure = {
        'name'               : unicode,
        'behavior'           : unicode,   # URL to Behavior JSON
        'cached_behavior'    : dict,      # Save the contents of behavior URL
        'particles'          : int,       # Number of particles to force
        'hydro_path'         : unicode,   # OPeNDAP or Local file path
        'geometry'           : unicode,   # WKT
        'release_depth'      : float,     # Release depth
        'start'              : datetime,  # Release in time
        'duration'           : int,       # Days
        'timestep'           : int,       # In seconds, the timestep between calculations
        'horiz_dispersion'   : float,     # Horizontal dispersion, in m/s
        'vert_dispersion'    : float,     # Horizontal dispersion, in m/s
        'time_chunk'         : int,
        'horiz_chunk'        : int,
        'time_method'        : unicode,   # Time method, 'nearest' or 'interp'
        'created'            : datetime,
        'task_id'            : unicode,
        'email'              : unicode,   # Email of the person who ran the model
        'output'             : list,
        'task_result'        : unicode,
        'trackline'          : unicode,   # GeoJSON
        'started'            : datetime,
        'ended'              : datetime,
        'shoreline_path'     : unicode,
        'shoreline_feature'  : unicode,
        'final_message'      : unicode,   # Message from the Job
    }
    default_values = {  'created': datetime.utcnow,
                        'time_chunk'  : 10,
                        'horiz_chunk' : 5,
                        'time_method' : u'interp' }
    migration_handler = RunMigration

    restrict_loading = ["output", "task_result", "trackline", "task_id", "created", "cached_behavior", "output", "started", "ended", "final_message", "_id"]

    def compute(self):
        """
        Add any metadata to this object from the model run output
        """
        try:
            self.set_trackline()
        except:
            app.logger.exception("Could not process trackline results.")

        if Job.exists(self.task_id, connection=redis_connection):
            job = Job.fetch(self.task_id, connection=redis_connection)
            self.task_result = unicode(job.meta.get("outcome", ""))
            self.final_message = unicode(job.meta.get("message", ""))

        self.save()

    def set_trackline(self):
        if self.trackline is None:
            for filepath in self.output:
                if os.path.basename(filepath) in ['trackline.geojson', 'simple_trackline.geojson']:
                    # Get GeoJSON trackline and cache locally as GeoJSON
                    try:
                        t = urllib2.urlopen(filepath)
                        self.trackline = unicode(geojson.loads(t.read()))
                    except ValueError:
                        t = open(filepath, 'r')
                        self.trackline = unicode(geojson.loads(t.read()))
                        t.close()

        return self.trackline

    def status(self):
        if self.task_result is not None and self.task_result != "":
            return self.task_result
        elif self.task_id and Job.exists(self.task_id, connection=redis_connection):
            job = Job.fetch(self.task_id, connection=redis_connection)
            return job.status
        else:
            return "unknown"

    def progress(self):
        if self.task_result is not None and self.task_result != "":
            return 100
        elif self.task_id and Job.exists(self.task_id, connection=redis_connection):
            job = Job.fetch(self.task_id, connection=redis_connection)
            return job.meta.get("progress", 0)
        else:
            return "unknown"

    def message(self):
        if self.final_message is not None and self.final_message != "":
            return self.final_message
        elif self.task_id and Job.exists(self.task_id, connection=redis_connection):
            job = Job.fetch(self.task_id, connection=redis_connection)
            return job.meta.get("message", None)
        else:
            return "unknown"

    def last_progress_update(self):
        if self.task_result is not None and self.task_result != "":
            return "run complete"
        elif self.task_id and Job.exists(self.task_id, connection=redis_connection):
            job = Job.fetch(self.task_id, connection=redis_connection)
            return job.meta.get("updated", None)
        else:
            return "unknown"

    def google_maps_trackline(self):
        if self.trackline:
            try:
                return list(geojson.utils.coords(geojson.loads(self.trackline)))
            except AttributeError:
                return []
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
            if 'cache' in name:
                file_type = "Forcing Data"
            else:
                file_type = "NetCDF"
        elif ext == ".json":
            file_type = "JSON"
        elif ext == ".geojson":
            if "particle_tracklines" in name:
                file_type = "Particle Tracklines (GeoJSON)"
            elif "simple_trackline" in name:
                file_type = "Simple Trackline (GeoJSON)"
            elif "full_trackline" in name:
                file_type = "Center Trackline (GeoJSON)"
            elif "particle_multipoint" in name:
                file_type = "Particle MultiPoint (GeoJSON)"
        elif ext == ".avi":
            file_type = "Animation"
        elif ext == ".log":
            file_type = "Logfile"
        elif ext == ".h5":
            file_type = "HDF5 Data File"

        return { file_type : file_link }

    def output_files(self):
        return (self.get_file_key_and_link(file_path) for file_path in self.output)

    def run_config(self):

        skip_keys = ['_id', 'cached_behavior', 'created', 'task_id', 'output', 'trackline', 'task_result', 'started', 'ended', 'final_message']
        d = {}
        for key, value in self.iteritems():
            if key not in skip_keys:
                if key == 'start':
                    d[key] = value.isoformat()
                else:
                    d[key] = value

        return d

    def load_run_config(self, run):
        from paegan.utils.datetime import datetime_parser
        # Set the 1:1 relationship between the config and this object
        for key, value in run.iteritems():

            # Don't save keys that shouldn't be part of an initial run object
            if key in Run.restrict_loading:
                continue

            if key == 'start':
                if isinstance(value, datetime):
                    d = value
                elif isinstance(value, date):
                    d = datetime.combine(value, datetime.min.time())
                elif isinstance(value, basestring):
                    d = datetime_parser(value)
                elif isinstance(value, (int, float)):
                    d = datetime.fromtimestamp(value / 1000, pytz.utc)

                # Convert to UTC
                if d.tzinfo is None:
                    d = d.replace(tzinfo=pytz.utc)
                self[key] = d.astimezone(pytz.utc)

            elif key == 'release_depth' or key == 'horiz_dispersion' or key == 'vert_dispersion':
                self[key] = float(value)

            else:
                try:
                    if value is not None:
                        setattr(self, key, value)
                except Exception:
                    app.logger.exception("Unknown run config key: %s.  Ignoring." % key)

        if self.behavior:
            try:
                b = urllib2.urlopen(self.behavior)
                self.cached_behavior = json.loads(b.read())
            except:
                pass


db.register([Run])
