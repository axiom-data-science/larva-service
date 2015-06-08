from larva_service import app, db, slugify, redis_connection
from flask import current_app
from shapely.wkt import loads
import tempfile
import collections
import multiprocessing
import pytz
import math
import os
import shutil
import logging
from datetime import datetime
from urlparse import urljoin

import threading

from paegan.transport.models.behavior import LarvaBehavior
from paegan.transport.models.transport import Transport
from paegan.transport.controllers import CachingModelController

from boto.s3.connection import S3Connection
from boto.s3.key import Key

from bson.objectid import ObjectId

from rq import get_current_job

import time

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

            def save_progress(stop, progress_deque, logger):
                while True:

                    if stop():
                        break
                    try:
                        record = progress_deque.pop()
                        if record == StopIteration:
                            break

                        job.meta["updated"] = record[0]
                        if record is not None and record[1] >= 0:
                            job.meta["progress"] = record[1]
                        if isinstance(record[2], unicode) or isinstance(record[2], str):
                            job.meta["message"] = record[2]

                        job.save()
                    except IndexError:
                        pass
                    except Exception:
                        raise

                    # Relax
                    time.sleep(0.1)

            # Set up Logger
            from paegan.logger.progress_handler import ProgressHandler
            from paegan.logger.multi_process_logging import MultiProcessingLogHandler
            from paegan.logger import logger

            # Logging handler
            queue = multiprocessing.Queue(-1)
            logger.setLevel(logging.PROGRESS)
            if app.config.get("DEBUG") is True:
                mphandler = MultiProcessingLogHandler(log_file, queue, stream=True)
            else:
                mphandler = MultiProcessingLogHandler(log_file, queue)
            mphandler.setLevel(logging.PROGRESS)
            formatter = logging.Formatter('[%(asctime)s] - %(levelname)s - %(name)s - %(processName)s - %(message)s')
            mphandler.setFormatter(formatter)
            logger.addHandler(mphandler)

            # Progress handler
            progress_deque = collections.deque(maxlen=1)
            progress_handler = ProgressHandler(progress_deque)
            progress_handler.setLevel(logging.PROGRESS)
            logger.addHandler(progress_handler)

            stop_log_listener = False
            pl = threading.Thread(name="ProgressUpdater", target=save_progress, args=(lambda: stop_log_listener, progress_deque, logger,))
            pl.start()

            model = CachingModelController(
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
                time_chunk=run['time_chunk'],
                horiz_chunk=run['horiz_chunk'],
                shoreline_path=shoreline_path,
                shoreline_feature=shoreline_feat,
                shoreline_index_buffer=0.05)
            cache_file = os.path.join(cache_path, run_id + ".nc.cache")
            model.setup_run(hydropath, cache_path=cache_file, remove_cache=False)

            run.started = datetime.utcnow()
            model.run(output_formats=output_formats, output_path=output_path)

        except Exception as e:
            app.logger.exception("Failed to run model")
            job.meta["message"]  = e.message
            job.meta["outcome"] = "failed"

        else:
            job.meta["message"]  = "Complete"
            job.meta["outcome"] = "success"
            job.meta["progress"] = 100

        finally:

            # Stop progress log handler
            stop_log_listener = True
            # Wait for log listener to exit
            pl.join()

            # Close and remove the handlers so we can use the log file without a file lock
            for hand in list(logger.handlers):
                logger.removeHandler(hand)
                hand.flush()
                hand.close()
            queue.close()
            queue.join_thread()

            # LOOK: Without this, the destination log file (model.log) is left
            # with an open file handler.  I'm baffled and can't figure out why.
            # Try removing this and running `lsof` on the output directory.
            # Yeah.  Mind Blown.
            time.sleep(1)

            # Move logfile to output directory
            shutil.move(log_file, os.path.join(output_path, 'model.log'))

            # Move cachefile to output directory if we made one
            shutil.move(cache_file, os.path.join(output_path, 'hydro_cache.nc'))

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
