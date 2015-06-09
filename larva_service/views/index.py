from flask import render_template, make_response, redirect
from larva_service import app
from larva_service.views.helpers import requires_auth


@app.route('/', methods=['GET'])
def index():

    show_keys = ['OUTPUT_PATH', 'SHORE_PATH', 'BATHY_PATH', 'CACHE_PATH', 'DEBUG', 'TESTING', 'NON_S3_OUTPUT_URL']
    environ = { g: v for g, v in app.config.items() if g in show_keys }

    return render_template('index.html', environment=environ)


@requires_auth
@app.route('/jobs', methods=['GET'])
def jobs():        
    return redirect('/rq')


@app.route('/crossdomain.xml', methods=['GET'])
def crossdomain():
    domain = """
    <cross-domain-policy>
        <allow-access-from domain="*"/>
        <site-control permitted-cross-domain-policies="all"/>
        <allow-http-request-headers-from domain="*" headers="*"/>
    </cross-domain-policy>
    """
    response = make_response(domain)
    response.headers["Content-type"] = "text/xml"
    return response
