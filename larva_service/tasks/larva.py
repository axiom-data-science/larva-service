from larva_service import app, db, slugify
from flask import current_app
from shapely.wkt import loads
import json
import tempfile
import pytz
import math
import sys
import os
import shutil
import multiprocessing
import logging
from datetime import datetime
from urlparse import urljoin, urlparse

import threading
import collections

from paegan.transport.models.behavior import LarvaBehavior
from paegan.transport.models.transport import Transport
from paegan.transport.model_controller import ModelController

from logging import FileHandler
from paegan.logger.progress_handler import ProgressHandler

from boto.s3.connection import S3Connection
from boto.s3.key import Key

from bson.objectid import ObjectId

from rq import get_current_job

import time

import redis

from larva_service.models.run import ResultsPyTable


def run(run_id):

    with app.app_context():

        job = get_current_job()

        output_path = os.path.join(current_app.config['OUTPUT_PATH'], run_id)
        shutil.rmtree(output_path, ignore_errors=True)
        os.makedirs(output_path)

        cache_path = os.path.join(current_app.config['CACHE_PATH'], run_id)
        shutil.rmtree(cache_path, ignore_errors=True)
        os.makedirs(cache_path)

        f, log_file = tempfile.mkstemp(dir=cache_path, prefix=run_id, suffix=".log")
        os.close(f)

        # Set up Logger
        logger = logging.getLogger(run_id)
        handler = FileHandler(log_file)
        handler.setLevel(logging.INFO)
        formatter = logging.Formatter('[%(asctime)s] - %(levelname)s - %(name)s - %(processName)s - %(message)s')
        handler.setFormatter(formatter)
        logger.addHandler(handler)

        res = urlparse(current_app.config.get("RESULTS_REDIS_URI"))
        redis_pool = redis.ConnectionPool(host=res.hostname, port=res.port, db=res.path[1:])
        r = redis.Redis(connection_pool=redis_pool)

        def listen_for_logs():
            pubsub = r.pubsub()
            pubsub.subscribe("%s:log" % run_id)
            for msg in pubsub.listen():
                if msg['type'] != "message":
                    continue

                if msg["data"] == "FINISHED":
                    break

                prog = json.loads(msg["data"])
                if prog is not None:
                    if prog.get("level", "").lower() == "progress":
                        job.meta["progress"] = prog.get("value", job.meta.get("progress", None))
                        job.meta["message"]  = prog.get("message", job.meta.get("message", ""))
                        job.meta["updated"]  = prog.get("time", datetime.utcnow().replace(tzinfo=pytz.utc).astimezone(pytz.utc))
                        job.save()
                        logger.info("PROGRESS: %(value)d - %(message)s" % prog)
                    else:
                        getattr(logger, prog["level"].lower())(prog.get("message"))

            pubsub.close()
            sys.exit()

        def listen_for_results():
            # Create output file (hdf5)
            results = ResultsPyTable(os.path.join(output_path, "results.h5"))
            pubsub = r.pubsub()
            pubsub.subscribe("%s:results" % run_id)
            for msg in pubsub.listen():
                if msg['type'] != "message":
                    continue

                if msg["data"] == "FINISHED":
                    break

                # Write to HDF file
                results.write(json.loads(msg["data"]))

            pubsub.close()
            results.compute()
            results.close()
            sys.exit()

        pl = threading.Thread(name="LogListener", target=listen_for_logs)
        pl.daemon = True
        pl.start()

        rl = threading.Thread(name="ResultListener", target=listen_for_results)
        rl.daemon = True
        rl.start()

        # Wait for PubSub listening to begin
        time.sleep(1)

        model = None
        try:

            #logger.progress((0, "Configuring model"))
            r.publish("%s:log" % run_id, json.dumps({"time" : datetime.utcnow().isoformat(), "level" : "progress", "value" : 0, "message" : "Configuring model"}))

            run = db.Run.find_one( { '_id' : ObjectId(run_id) } )
            if run is None:
                return "Failed to locate run %s. May have been deleted while task was in the queue?" % run_id

            geometry       = loads(run['geometry'])
            start_depth    = run['release_depth']
            num_particles  = run['particles']
            time_step      = run['timestep']
            num_steps      = int(math.ceil((run['duration'] * 24 * 60 * 60) / time_step))
            start_time     = run['start'].replace(tzinfo = pytz.utc)
            shoreline_path = run['shoreline_path'] or app.config.get("SHORE_PATH")
            shoreline_feat = run['shoreline_feature']

            # Setup Models
            models = []
            if run['cached_behavior'] is not None and run['cached_behavior'].get('results', None) is not None:
                behavior_data = run['cached_behavior']['results'][0]
                l = LarvaBehavior(data=behavior_data)
                models.append(l)
            models.append(Transport(horizDisp=run['horiz_dispersion'], vertDisp=run['vert_dispersion']))

            # Setup ModelController
            model = ModelController(geometry=geometry, depth=start_depth, start=start_time, step=time_step, nstep=num_steps, npart=num_particles, models=models, use_bathymetry=True, use_shoreline=True,
                                    time_chunk=run['time_chunk'], horiz_chunk=run['horiz_chunk'], time_method=run['time_method'], shoreline_path=shoreline_path, shoreline_feature=shoreline_feat, reverse_distance=1500)

            # Run the model
            cache_file = os.path.join(cache_path, run_id + ".nc.cache")
            bathy_file = current_app.config['BATHY_PATH']

            model.run(run['hydro_path'], output_formats=["redis", "trackline"], output_path=output_path, redis_url=current_app.config.get("RESULTS_REDIS_URI"), redis_results_channel="%s:results" % run_id, redis_log_channel="%s:log" % run_id, bathy=bathy_file, cache=cache_file, remove_cache=False, caching=run['caching'])

            job.meta["outcome"] = "success"
            job.save()
            return "Successfully ran %s" % run_id

        except Exception as exception:
            logger.warn("Run FAILED, cleaning up and uploading log.")
            logger.warn(exception.message)
            job.meta["outcome"] = "failed"
            job.save()
            raise

        finally:

            r.publish("%s:log" % run_id, json.dumps({"time" : datetime.utcnow().isoformat(), "level" : "progress", "value" : 99, "message" : "Processing output files and cleaning up"}))

            # Add a finished to the end.
            r.publish("%s:log" % run_id, "FINISHED")
            r.publish("%s:results" % run_id, "FINISHED")
            # Wait for results to be written
            pl.join()
            rl.join()
            # Remove all connections
            redis_pool.disconnect()

            # Close and remove the handlers so we can use the log file without a file lock
            for hand in list(logger.handlers):
                logger.removeHandler(hand)
                hand.flush()
                hand.close()
                del hand

            # LOOK: Without this, the destination log file (mode.log) is left
            # with an open file handler.  I'm baffled and can't figure out why.
            # Try removing this and running `lsof` on the output directory.
            # Yeah.  Mind Blown.
            time.sleep(1)

            # Move logfile to output directory
            shutil.move(log_file, os.path.join(output_path, 'model.log'))

            # Move cachefile to output directory if we made one
            if run['caching']:
                shutil.move(cache_file, output_path)

            output_files = []
            for filename in os.listdir(output_path):
                outfile = os.path.join(output_path, filename)
                output_files.append(outfile)

            result_files = []
            # Handle results and cleanup
            if current_app.config['USE_S3'] is True:
                base_access_url = urljoin("http://%s.s3.amazonaws.com/output/" % current_app.config['S3_BUCKET'], run_id)
                # Upload results to S3 and remove the local copies
                conn = S3Connection()
                bucket = conn.get_bucket(current_app.config['S3_BUCKET'])

                for outfile in output_files:
                    # Don't upload the cache file
                    if os.path.basename(outfile) == os.path.basename(cache_file):
                        continue

                    # Upload the outfile with the same as the run name
                    _, ext = os.path.splitext(outfile)
                    new_filename = slugify(unicode(run['name'])) + ext

                    k = Key(bucket)
                    k.key = "output/%s/%s" % (run_id, new_filename)
                    k.set_contents_from_filename(outfile)
                    k.set_acl('public-read')
                    result_files.append(base_access_url + "/" + new_filename)
                    os.remove(outfile)

                shutil.rmtree(output_path, ignore_errors=True)

            else:
                result_files = output_files

            # Set output fields
            run.output = result_files
            run.ended = datetime.utcnow()
            run.compute()
            run.save()

            del model

            job.meta["message"] = "Complete"
            job.save()
