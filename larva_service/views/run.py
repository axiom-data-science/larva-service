import os
import json
from urlparse import urlparse

import requests

from rq import cancel_job
from pymongo import DESCENDING
from flask import render_template, redirect, url_for, request, flash, jsonify, send_file, abort


from larva_service import app, db, run_queue, redis_connection, cache
from larva_service.models import remove_mongo_keys
from larva_service.models.run import Run

from larva_service.views.helpers import requires_auth
from larva_service.tasks.local import run as local_run
from larva_service.tasks.distributed import run as distributed_run

from flask_wtf import Form
from wtforms.fields import StringField, RadioField, TextAreaField, SelectField
from wtforms.fields.html5 import URLField, DateField, IntegerField, DecimalField, EmailField
from wtforms.validators import DataRequired, url, Email, NumberRange, Optional, InputRequired


def dataset_choices():
    with app.app_context():
        return [(s.location, '{}: {:%b %d %Y} to {:%b %d %Y}'.format(s.name, s.starting, s.ending) ) for s in db.Dataset.find().sort('name')]


def shoreline_choices():
    with app.app_context():
        return [(s.path, s.name) for s in db.Shoreline.find().sort('name')]


class RunForm(Form):
    name             = StringField('Name', validators=[ DataRequired() ], description='A unique and human readable name for this run, ie. "Montague Island 2006 - PWS Hindcast"')
    behavior         = SelectField('Behavior', validators=[ Optional() ])
    particles        = IntegerField('Particles', validators=[ NumberRange(min=1, max=200, message="Must be between 1 and 200.") ], description="The number of particles to force")
    dataset          = RadioField('Hydro model', validators=[ DataRequired() ], choices=dataset_choices())
    geometry         = TextAreaField('Starting position', validators=[ DataRequired() ], description="Point or Polygon geometry as a WKT string. Create using http://arthur-e.github.io/Wicket/sandbox-gmaps3.html.")
    release_depth    = DecimalField("Release depth", validators=[ InputRequired() ], description="Starting depth, in meters")
    start            = DateField('Start', validators=[ DataRequired() ])
    duration         = IntegerField('Duration', validators=[ DataRequired() ], description='Duration of run, in days')
    timestep         = IntegerField('Timestep', validators=[ DataRequired() ], default=3600, description='Seconds between movemement calculations')
    horiz_dispersion = DecimalField('Horizonal Dispersion', default=0., description='Horizontal dispersion coeficcient (m/s)')
    vert_dispersion  = DecimalField('Vertical Dispersion', default=0., description='Vertical dispersion coeficcient (m/s)')
    time_chunk       = IntegerField('Time cache', default=Run.default_values['time_chunk'], description="Tune the local Time cache when using DAP models")
    horiz_chunk      = IntegerField('Grid cache', default=Run.default_values['horiz_chunk'], description="Tune the local Grid cache when using DAP models")
    time_method      = RadioField('Time method', choices=[('nearest', 'Nearest'), ('interp', 'Interpolate')], default=Run.default_values['time_method'])
    email            = EmailField('Email', validators=[ Email() ])
    shoreline        = RadioField('Shoreline', validators=[ DataRequired() ], choices=shoreline_choices())


@app.route('/run', methods=['GET', 'POST'])
@app.route('/run.<string:format>', methods=['GET', 'POST'])
def run_larva_model(format=None):

    if format is None:
        format = 'html'

    if request.method == 'GET':
        run_details = request.args.get("config", None)
    elif request.method == 'POST':
        run_details = request.form.get("config", None)

    config_dict = None
    try:
        config_dict = json.loads(run_details.strip())

    except:
        message = "Could not decode parameters"
        if format == 'html':
            flash(message, 'danger')
            return redirect(url_for('runs'))
        elif format == 'json':
            return jsonify( { 'results' : message } )

    run = db.Run()
    run.load_run_config(config_dict)
    run.save()

    # Enqueue

    if urlparse(run.hydro_path).scheme != '':
        # DAP, use the CachingModelController
        job = run_queue.enqueue_call(func=local_run, args=(unicode(run['_id']),))
    else:
        # Local file path, use the DistributedModelController
        job = run_queue.enqueue_call(func=distributed_run, args=(unicode(run['_id']),))

    run.task_id = unicode(job.id)
    run.save()

    message = "Run created"
    if format == 'html':
        flash(message, 'success')
        return redirect(url_for('runs'))
    elif format == 'json':
        return jsonify( { 'results' : unicode(run['_id']) } )


@app.route('/submit', methods=['GET', 'POST'])
def submit():

    def behavior_choices():
        bh_root = app.config.get('BEHAVIOR_ROOT')
        if cache.get('behavior_list'):
            return cache.get('behavior_list')
        bh = requests.get('{}/library.json'.format(bh_root)).json()
        behavior_list = [ ('{}/library/{}.json'.format(bh_root, j['_id']), '{} - {}'.format(j['user'], j['name'])) for j in bh['results'] ]
        behavior_list = sorted(behavior_list, key=lambda x: x[1])
        behavior_list.insert(0, ('', 'None' ))
        cache.set('behavior_list', behavior_list)
        return behavior_list

    form = RunForm()
    form.behavior.choices = behavior_choices()

    if not form.validate_on_submit():
        return render_template('submit.html', form=form)

    try:
        # Translate from Form to the JSON that can be POSTed to /run here
        config_dict = dict()

        # Shoreline
        shoreline = db.Shoreline.find_one(name=form.shoreline.data)
        config_dict['shoreline_path'] = shoreline.path
        if shoreline.feature_name:
            config_dict['shoreline_feature'] = shoreline.feature_name

        # Dataset
        dataset = db.Dataset.find_one(name=form.dataset.data)
        config_dict['hydro_path'] = dataset.location

        if form.behavior.data:
            config_dict['behavior'] = form.behavior.data

        config_dict['name'] = form.name.data
        config_dict['particles'] = form.particles.data
        config_dict['geometry'] = form.geometry.data
        config_dict['release_depth'] = form.release_depth.data
        config_dict['release_depth'] = form.release_depth.data
        config_dict['start'] = form.start.data
        config_dict['duration'] = form.duration.data
        config_dict['timestep'] = form.timestep.data
        config_dict['horiz_dispersion'] = form.horiz_dispersion.data
        config_dict['vert_dispersion'] = form.vert_dispersion.data
        config_dict['time_chunk'] = form.time_chunk.data
        config_dict['horiz_chunk'] = form.horiz_chunk.data
        config_dict['time_method'] = form.time_method.data
        config_dict['email'] = form.email.data

        app.logger.info(config_dict)

    except BaseException as e:
        flash(e.message, 'info')
        return render_template('submit.html', form=form)

    else:
        run = db.Run()
        run.load_run_config(config_dict)
        run.save()
        # Enqueue
        if urlparse(run.hydro_path).scheme != '':
            # DAP, use the CachingModelController
            job = run_queue.enqueue_call(func=local_run, args=(unicode(run['_id']),))
        else:
            # Local file path, use the DistributedModelController
            job = run_queue.enqueue_call(func=distributed_run, args=(unicode(run['_id']),))
        run.task_id = unicode(job.id)
        run.save()
        flash('Run created', 'success')
        return redirect(url_for('runs'))


@app.route('/runs/<ObjectId:run_id>/delete', methods=['GET', 'DELETE'])
@app.route('/runs/<ObjectId:run_id>/delete.<string:format>', methods=['GET', 'DELETE'])
@requires_auth
def delete_run(run_id, format=None):
    if format is None:
        format = 'html'

    run = db.Run.find_one( { '_id' : run_id } )
    cancel_job(run.task_id, connection=redis_connection)
    run.delete()

    if format == 'json':
        return jsonify( { 'status' : "success" })
    else:
        flash("Run deleted", 'success')
        return redirect(url_for('runs'))


@app.route('/runs/clear', methods=['GET'])
@requires_auth
def clear_runs():
    db.drop_collection("runs")
    return redirect(url_for('runs'))


@app.route('/runs', methods=['GET'])
@app.route('/runs.<string:format>', methods=['GET'])
def runs(format=None):
    if format is None:
        format = 'html'

    runs = db.Run.find().sort('created', DESCENDING)

    if format == 'html':
        return render_template('runs.html', runs=runs)
    elif format == 'json':
        jsond = []
        for run in runs:
            js = json.loads(run.to_json())
            remove_mongo_keys(js, extra=['output', 'cached_behavior', 'task_result', 'task_id'])
            js['_id'] = unicode(run._id)
            js['status'] = unicode(run.status())
            js['output'] = list(run.output_files())
            jsond.append(js)
        return jsonify( { 'results' : jsond } )
    else:
        flash("Reponse format '%s' not supported" % format, 'warning')
        return redirect(url_for('runs'))


@app.route('/runs/<ObjectId:run_id>', methods=['GET'])
@app.route('/runs/<ObjectId:run_id>.<string:format>', methods=['GET'])
def show_run(run_id, format=None):
    if format is None:
        format = 'html'

    run = db.Run.find_one( { '_id' : run_id } )

    if format == 'html':
        markers = run.google_maps_coordinates()
        linestring = run.google_maps_trackline()
        run_config = json.dumps(run.run_config(), sort_keys=True, indent=4)
        cached_behavior = json.dumps(run.cached_behavior, sort_keys=True, indent=4)
        return render_template('show_run.html', run=run, run_config=run_config, cached_behavior=cached_behavior, line=linestring, markers=markers)
    elif format == 'json':
        jsond = json.loads(run.to_json())
        remove_mongo_keys(jsond, extra=['output', 'task_result', 'task_id'])
        jsond['_id'] = unicode(run._id)
        jsond['status'] = unicode(run.status())
        jsond['output'] = list(run.output_files())
        return jsonify( jsond )
    else:
        flash("Reponse format '%s' not supported" % format, 'warning')
        return redirect(url_for('runs'))


@app.route('/runs/<ObjectId:run_id>/status', methods=['GET'])
@app.route('/runs/<ObjectId:run_id>/status.<string:format>', methods=['GET'])
def status_run(run_id, format=None):
    if format is None:
        format = 'json'

    run = db.Run.find_one( { '_id' : run_id } )
    run_status = run.status()

    if format == 'json':
        return jsonify( { 'status' : run_status })
    else:
        flash("Reponse format '%s' not supported" % format, 'warning')
        return redirect(url_for('runs'))


@app.route('/runs/<ObjectId:run_id>/run_config', methods=['GET'])
def run_config(run_id):
    run = db.Run.find_one( { '_id' : run_id } )
    return jsonify( run.run_config() )


@app.route("/runs/<ObjectId:run_id>/output/<string:filename>", methods=['GET'])
def run_output_download(run_id, filename):
    # Avoid being able to download ".." and "/"
    if '..' in filename or filename.startswith('/'):
        abort(404)

    run = db.Run.find_one( { '_id' : run_id } )
    for f in run.output:
        if os.path.basename(f) == filename:
            return send_file(f)
