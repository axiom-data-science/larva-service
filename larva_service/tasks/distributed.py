from larva_service import app, db, slugify, particle_queue, redis_connection
from flask import current_app
from shapely.wkt import loads
import json
import tempfile
import collections
import multiprocessing
import pytz
import math
import sys
import os
import shutil
import logging
from datetime import datetime
from urlparse import urljoin, urlparse

import threading

from paegan.transport.models.behavior import LarvaBehavior
from paegan.transport.models.transport import Transport
from paegan.transport.controllers import DistributedModelController, CachingModelController

from logging import FileHandler

from boto.s3.connection import S3Connection
from boto.s3.key import Key

from bson.objectid import ObjectId

from rq import get_current_job

import time

import redis

import paegan.transport.export as ex


def run(run_id):

    with app.app_context():

        job = get_current_job(connection=redis_connection)

        output_path = os.path.join(current_app.config['OUTPUT_PATH'], run_id)
        shutil.rmtree(output_path, ignore_errors=True)
        os.makedirs(output_path)

        cache_path = os.path.join(current_app.config['CACHE_PATH'], run_id)
        shutil.rmtree(cache_path, ignore_errors=True)
        os.makedirs(cache_path)

        f, log_file = tempfile.mkstemp(dir=cache_path, prefix=run_id, suffix=".log")
        os.close(f)
        os.chmod(log_file, 0644)

        run = db.Run.find_one( { '_id' : ObjectId(run_id) } )
        if run is None:
            return "Failed to locate run %s. May have been deleted while task was in the queue?" % run_id

        # Wait for PubSub listening to begin
        time.sleep(2)

        model = None
        try:

            hydropath      = run['hydro_path']
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

            output_formats = [
                ex.H5Trackline,
                ex.H5TracklineWithPoints,
                ex.H5ParticleTracklines,
                ex.H5ParticleMultiPoint,
                ex.H5GDALShapefile
            ]

            def listen_for_logs(redis_url, log_channel, stop):

                res = urlparse(redis_url)
                redis_pool = redis.ConnectionPool(host=res.hostname, port=res.port, db=res.path[1:])
                r = redis.Redis(connection_pool=redis_pool)

                # Set up Logger
                logger = logging.getLogger(run_id)
                logger.setLevel(logging.PROGRESS)
                handler = FileHandler(log_file)
                handler.setLevel(logging.PROGRESS)
                formatter = logging.Formatter('[%(asctime)s] - %(levelname)s - %(name)s - %(processName)s - %(message)s')
                handler.setFormatter(formatter)
                logger.addHandler(handler)

                pubsub = r.pubsub()
                pubsub.subscribe(log_channel)

                while True:
                    if stop():
                        break
                    msg = pubsub.get_message()
                    if msg:
                        if msg['type'] != "message":
                            continue

                        try:
                            prog = json.loads(msg["data"])
                            if prog is not None:
                                if prog.get("level", "").lower() == "progress":
                                    job.meta["progress"] = float(prog.get("value", job.meta.get("progress", None)))
                                    job.meta["message"]  = prog.get("message", job.meta.get("message", ""))
                                    job.meta["updated"]  = prog.get("time", datetime.utcnow().replace(tzinfo=pytz.utc).astimezone(pytz.utc))
                                    job.save()
                                    logger.progress("%(value).2f - %(message)s" % prog)
                                else:
                                    getattr(logger, prog["level"].lower())(prog.get("message"))
                        except Exception:
                            logger.info("Got strange result: %s" % msg["data"])

                    # Relax
                    time.sleep(0.01)

                pubsub.unsubscribe()
                pubsub.close()
                # Close and remove the handlers so we can use the log file without a file lock
                for hand in list(logger.handlers):
                    logger.removeHandler(hand)
                    hand.flush()
                    hand.close()
                    del hand
                # Remove all connections
                redis_pool.disconnect()
                sys.exit()

            stop_log_listener = False
            log_channel = "{}:log".format(run_id)
            results_channel = "{}:results".format(run_id)
            pl = threading.Thread(name="LogListener", target=listen_for_logs, args=(current_app.config.get("RESULTS_REDIS_URI"), log_channel, lambda: stop_log_listener, ))
            pl.daemon = True
            pl.start()

            model = DistributedModelController(
                geometry=geometry,
                depth=start_depth,
                start=start_time,
                step=time_step,
                nstep=num_steps,
                npart=num_particles,
                models=models,
                use_bathymetry=True,
                bathy_path=current_app.config['BATHY_PATH'],
                use_shoreline=True,
                time_method=run['time_method'],
                shoreline_path=shoreline_path,
                shoreline_feature=shoreline_feat,
                shoreline_index_buffer=0.05)
            model.setup_run(hydropath, redis_url=current_app.config.get("RESULTS_REDIS_URI"), redis_results_channel=results_channel, redis_log_channel=log_channel)

            run.started = datetime.utcnow()
            model.run(output_formats=output_formats, output_path=output_path, task_queue_call=particle_queue.enqueue_call)

        except Exception as e:
            app.logger.exception("Failed to run model")
            job.meta["message"]  = e.message
            job.meta["outcome"] = "failed"

        else:
            job.meta["message"]  = "Complete"
            job.meta["outcome"] = "success"
            job.meta["progress"] = 100

        finally:

            # Send message to the log listener to finish
            stop_log_listener = True
            # Wait for log listener to exit
            pl.join()

            # LOOK: Without this, the destination log file (model.log) is left
            # with an open file handler.  I'm baffled and can't figure out why.
            # Try removing this and running `lsof` on the output directory.
            # Yeah.  Mind Blown.
            time.sleep(1)

            # Move logfile to output directory
            shutil.move(log_file, os.path.join(output_path, 'model.log'))

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

            job.meta["updated"]  = datetime.utcnow().replace(tzinfo=pytz.utc).astimezone(pytz.utc)
            job.save()

            # Set output fields
            run.output = result_files
            run.ended = datetime.utcnow()
            run.compute()
            run.save()
