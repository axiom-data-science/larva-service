from larva_service import app, db, slugify, particle_queue
from flask import current_app
from shapely.wkt import loads
import json
import tempfile
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
from paegan.transport.forcers import BaseForcer
from paegan.transport.model_controller import DistributedModelController

from logging import FileHandler

from boto.s3.connection import S3Connection
from boto.s3.key import Key

from bson.objectid import ObjectId

from rq import get_current_job

import time

import redis

from larva_service.models.run import ResultsPyTable

import paegan.transport.export as ex


def particle(hydrodataset, part, model):

    from paegan.logger import logger
    from paegan.logger.redis_handler import RedisHandler
    rhandler = RedisHandler(model.redis_log_channel, model.redis_url)
    rhandler.setLevel(logging.PROGRESS)
    logger.addHandler(rhandler)

    try:
        redis_connection = redis.from_url(model.redis_url)
        forcer = BaseForcer(hydrodataset,
                            particle=part,
                            common_variables=model.common_variables,
                            times=model.times,
                            start_time=model.start,
                            models=model._models,
                            release_location_centroid=model.reference_location.point,
                            usebathy=model._use_bathymetry,
                            useshore=model._use_shoreline,
                            usesurface=model._use_seasurface,
                            reverse_distance=model.reverse_distance,
                            bathy_path=model.bathy_path,
                            shoreline_path=model.shoreline_path,
                            shoreline_feature=model.shoreline_feature,
                            time_method=model.time_method,
                            redis_url=model.redis_url,
                            redis_results_channel=model.redis_results_channel,
                            shoreline_index_buffer=model.shoreline_index_buffer
                           )
        forcer.run()
    except Exception:
        redis_connection.publish(model.redis_results_channel, json.dumps({"status" : "FAILED", "uid" : part.uid }))
    else:
        redis_connection.publish(model.redis_results_channel, json.dumps({"status" : "COMPLETED", "uid" : part.uid }))


def manager(run_id):

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
        os.chmod(log_file, 0644)

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

        run = db.Run.find_one( { '_id' : ObjectId(run_id) } )
        if run is None:
            return "Failed to locate run %s. May have been deleted while task was in the queue?" % run_id

        def listen_for_logs():
            pubsub = r.pubsub()
            pubsub.subscribe("%s:log" % run_id)
            for msg in pubsub.listen():

                if msg['type'] != "message":
                    continue

                if msg["data"] == "FINISHED":
                    break

                try:
                    prog = json.loads(msg["data"])
                    if prog is not None:
                        if prog.get("level", "").lower() == "progress":
                            job.meta["progress"] = float(prog.get("value", job.meta.get("progress", None)))
                            job.meta["message"]  = prog.get("message", job.meta.get("message", ""))
                            job.meta["updated"]  = prog.get("time", datetime.utcnow().replace(tzinfo=pytz.utc).astimezone(pytz.utc))
                            job.save()
                            logger.info("PROGRESS: %(value).2f - %(message)s" % prog)
                        else:
                            getattr(logger, prog["level"].lower())(prog.get("message"))
                except Exception:
                    logger.info("Got strange result: %s" % msg["data"])
                    pass

            pubsub.close()
            sys.exit()

        def listen_for_results(output_h5_file, total_particles):
            # Create output file (hdf5)
            particles_finished = 0
            results = ResultsPyTable(output_h5_file)
            pubsub = r.pubsub()
            pubsub.subscribe("%s:results" % run_id)
            for msg in pubsub.listen():

                if msg['type'] != "message":
                    continue

                if msg["data"] == "FINISHED":
                    break

                try:
                    json_msg = json.loads(msg["data"])
                    if json_msg.get("status", None):
                        #  "COMPLETED" or "FAILED" when a particle finishes
                        particles_finished += 1
                        percent_complete = 90. * (float(particles_finished) / float(total_particles)) + 5  # Add the 5 progress that was used prior to the particles starting (controller)
                        r.publish("%s:log" % run_id, json.dumps({"time" : datetime.utcnow().isoformat(), "level" : "progress", "value" : percent_complete, "message" : "Particle #%s %s!" % (particles_finished, json_msg.get("status"))}))
                        if particles_finished == total_particles:
                            break
                    else:
                        # Write to HDF file
                        results.write(json_msg)
                except Exception:
                    logger.info("Got strange result: %s" % msg["data"])
                    pass

            pubsub.close()
            results.compute()
            results.close()
            sys.exit()

        pl = threading.Thread(name="LogListener", target=listen_for_logs)
        pl.daemon = True
        pl.start()

        output_h5_file = os.path.join(output_path, "results.h5")
        rl = threading.Thread(name="ResultListener", target=listen_for_results, args=(output_h5_file, run['particles']))
        rl.daemon = True
        rl.start()

        # Wait for PubSub listening to begin
        time.sleep(1)

        model = None
        try:

            r.publish("%s:log" % run_id, json.dumps({"time" : datetime.utcnow().isoformat(), "level" : "progress", "value" : 0, "message" : "Setting up model"}))

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

            model = DistributedModelController(geometry=geometry,
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

            model.setup_run(hydropath, output_formats=["redis"], redis_url=current_app.config.get("RESULTS_REDIS_URI"), redis_results_channel="%s:results" % run_id, redis_log_channel="%s:log" % run_id)

        except Exception as exception:
            logger.warn("Run failed to initialize, cleaning up.")
            logger.warn(exception.message)
            job.meta["outcome"] = "failed"
            job.save()
            raise

        try:
            r.publish("%s:log" % run_id, json.dumps({"time" : datetime.utcnow().isoformat(), "level" : "progress", "value" : 4, "message" : "Adding particles to queue"}))
            for part in model.particles:
                particle_queue.enqueue_call(func=particle, args=(hydropath, part, model,))

        except Exception, exception:
            logger.warn("Failed to start particles, cleaning up.")
            logger.warn(exception.message)
            r.publish("%s:results" % run_id, "FINISHED")
            job.meta["outcome"] = "failed"
            job.save()
            raise

        finally:

            r.publish("%s:log" % run_id, json.dumps({"time" : datetime.utcnow().isoformat(), "level" : "progress", "value" : 5, "message" : "Waiting for particles to finish..."}))
            # Wait for results to be written.  This thread exits when it has recieved a message from all particle runners
            rl.join()

            r.publish("%s:log" % run_id, json.dumps({"time" : datetime.utcnow().isoformat(), "level" : "progress", "value" : 96, "message" : "Processing output files and cleaning up"}))
            # Send message to the log listener to finish
            r.publish("%s:log" % run_id, "FINISHED")
            # Wait for log listener to exit
            pl.join()

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

            # Compute common output from HDF5 file and put in output_path
            ex.H5Trackline.export(folder=output_path, h5_file=output_h5_file)
            ex.H5ParticleTracklines.export(folder=output_path, h5_file=output_h5_file)
            ex.H5ParticleMultiPoint.export(folder=output_path, h5_file=output_h5_file)
            ex.H5GDALShapefile.export(folder=output_path, h5_file=output_h5_file)

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

            job.meta["outcome"] = "success"
            job.meta["progress"] = 100
            job.meta["updated"]  = datetime.utcnow().replace(tzinfo=pytz.utc).astimezone(pytz.utc)
            job.meta["message"]  = "Complete"
            job.save()

            # Set output fields
            run.output = result_files
            run.ended = datetime.utcnow()
            run.compute()
            run.save()

            del model
