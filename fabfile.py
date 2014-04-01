from fabric.api import *
from fabric.contrib.files import *
from fabtools.system import cpus
from copy import copy
import time

"""
    Call this with fab -c .fab TASK to pick up deploy variables
    Required variables in .fab file:
        webpass = your_web_password
        secret_key = your_secret_key
        mongo_db = mongodb://localhost:27017/larvaservice_production
        redis_db = redis://localhost:6379/0
        results_redis_db = redis://localhost:6379/0
        webuser = www-data ('nginx' for RedHat based distros)

        use_s3 = False
        s3_bucket = notneeded
        aws_access = notneeded
        aws_secret = notneeded

        non_s3_output_url = "http://particles.pvd.axiomalaska.com/files/"

        bathy_path  = "/data/bathy/world_etopo1/ETOPO1_Bed_g_gmt4.grd"
        output_path = "/mnt/gluster/data/particle_output"
        shore_path  = "/data/shore/westcoast/New_Land_Clean.shp"
        distribute  = True
"""

env.user = "larva"

code_dir = "/home/larva/larva-service"
env["code_dir"] = code_dir

env.roledefs.update({
    'setup'     : ["larva04.axiom", "larva05.axiom", "larva06.axiom"],
    'web'       : [],
    'datasets'  : [],
    'shorelines': [],
    'runs'      : [],
    'workers'   : ["larva01.axiom"],
    'particles' : ["larva02.axiom", "larva03.axiom", "larva04.axiom", "larva05.axiom", "larva06.axiom"],
    #'all'       : ["calcium.axiompvd"]
})


# For copy and pasting when running tasks system wide
# @roles('web','datasets','shorelines','runs','particles','workers','all')


def admin():
    env.user = "axiom"


def larva():
    env.user = "larva"


@roles('workers')
def deploy_workers():
    stop_supervisord()
    larva()
    with cd(code_dir):
        update_code()
        update_libs()
        update_supervisord()
        start_supervisord()
        run("supervisorctl -c ~/supervisord.conf start runs")
        run("supervisorctl -c ~/supervisord.conf start datasets")
        run("supervisorctl -c ~/supervisord.conf start gunicorn")
        run("supervisorctl -c ~/supervisord.conf start shorelines")
        num_cpus = cpus() - 4
        for i in xrange(num_cpus):
            run("supervisorctl -c ~/supervisord.conf start particles:%s" % i)


@roles('particles')
def deploy_particles():
    stop_supervisord()
    larva()
    with cd(code_dir):
        update_code()
        update_libs()
        update_supervisord()
        start_supervisord()
        for i in xrange(cpus()):
            run("supervisorctl -c ~/supervisord.conf start particles:%s" % i)


@roles('setup')
def setup_debian():
     # Based on Debian Wheezy
    admin()

    # Install additonal packages
    sudo("apt-get install -y gfortran liblzo2-dev libbz2-dev libblas-dev liblapack-dev curl libgdal-dev libproj-dev libgeos-dev libgeos++-dev git nginx python2.7 python2.7-dev gcc g++ make libfreetype6-dev libpng-dev libtiff-dev libjpeg-dev")

    # Setup larva user
    #setup_larva_user()

    # Setup the python virtualenv\
    #with settings(warn_only=True):
    #    setup_burrito()

    # Get code
    #setup_code()

    # Get NetCDF libraries for Debian
    #update_netcdf_libraries_debian()

    # Process requirements.txt
    install_requirements()

    # Setup Nginx
    execute(setup_nginx)

    # Setup supervisord
    update_supervisord()


@roles('web', 'workers')
def setup_nginx():
    admin()
    upload_template('deploy/nginx.conf', '/etc/nginx/nginx.conf', context=copy(env), use_sudo=True, backup=False, mirror_local_mode=True)
    upload_template('deploy/nginx_larva.conf', '/etc/nginx/conf.d/larva.conf', context=copy(env), use_sudo=True, backup=False, mirror_local_mode=True)
    sudo("insserv nginx")
    sudo("/etc/init.d/nginx restart")


@roles('web', 'datasets', 'shorelines', 'runs', 'particles', 'workers', 'all')
def update_supervisord():
    larva()
    run("pip install supervisor")
    num_cpus = cpus()
    if env["host"] == "larva01.axiom":
        num_cpus -= 4
    env["system_cpus"] = num_cpus
    upload_template('deploy/supervisord.conf', '/home/larva/supervisord.conf', context=copy(env), use_jinja=True, use_sudo=False, backup=False, mirror_local_mode=True, template_dir='.')


def setup_code():
    larva()
    with cd("~"):
        run("rm -rf larva-service")
        run("git clone https://github.com/kwilcox/larva-service.git")


def update_code():
    larva()
    with cd(code_dir):
        run("git pull origin master")


def install_requirements():
    larva()
    update_code()
    with cd("~"):
        # We do these seperately becasue they are Pytables requirements
        run("pip install numpy==1.8.0")
        run("pip install numexpr")
        run("pip install cython")

        # Pytables.  Ugh.  v.3.1.0
        #run("rm -rf PyTables")
        #run("git clone https://github.com/PyTables/PyTables.git")
        with cd("PyTables"):
            run("git checkout v.3.1.0")
            run('HDF5_DIR=/opt/hdf5-1.8.12 python setup.py install --hdf5=/opt/hdf5-1.8.12 --lflags="-Xlinker -rpath -Xlinker /opt/hdf5-1.8.12/lib" --cflags="-w -O3 -msse2"')

    with cd(code_dir):
        run("HDF5_DIR=/opt/hdf5-1.8.12 NETCDF4_DIR=/opt/netcdf-4.3.1 pip install netCDF4")
        update_libs()


def update_netcdf_libraries_rh():
    admin()
    run("cd ~")
    run("wget https://asa-dev.s3.amazonaws.com/installNCO.txt")
    run("chmod 744 installNCO.txt")
    sudo("./installNCO.txt")


def update_netcdf_libraries_debian():
    admin()
    put(local_path='deploy/debian_netcdf.sh', remote_path='/root/debian_netcdf.sh', use_sudo=True, mirror_local_mode=True)
    sudo("bash /root/debian_netcdf.sh")


def setup_burrito():
    larva()
    run("curl -s https://raw.github.com/brainsik/virtualenv-burrito/master/virtualenv-burrito.sh | $SHELL")
    run("mkvirtualenv -p /usr/bin/python2.7 larva")
    run("echo 'workon larva' >> ~/.bash_profile")


def setup_larva_user():
    admin()
    # Setup larva user
    sudo("useradd -s /bin/bash -d /home/larva larva", warn_only=True)
    sudo("mkdir -p /home/larva/.ssh", warn_only=True)
    upload_key_to_larva()
    sudo("chown -R larva:larva /home/larva/")


def update_libs():
    larva()
    with cd(code_dir):
        with settings(warn_only=True):
            run("pip install -e git+https://github.com/kwilcox/paegan.git@master#egg=paegan")
            run("CPLUS_INCLUDE_PATH=/usr/include/gdal C_INCLUDE_PATH=/usr/include/gdal pip install -e git+https://github.com/kwilcox/paegan-transport.git@master#egg=paegan-transport")
            run("pip install -e git+https://github.com/kwilcox/paegan-viz.git@master#egg=paegan-viz")
            run("pip install -r requirements.txt")


def supervisord_restart():
    stop_supervisord()
    start_supervisord()


def stop_supervisord():
    larva()
    with cd(code_dir):
        with settings(warn_only=True):
            run("supervisorctl -c ~/supervisord.conf stop all")
            run("kill -QUIT $(ps aux | grep supervisord | grep -v grep | awk '{print $2}')")

    kill_pythons()


def kill_pythons():
    admin()
    with settings(warn_only=True):
        sudo("kill -QUIT $(ps aux | grep python | grep -v supervisord | awk '{print $2}')")


def start_supervisord():
    larva()
    with cd(code_dir):
        with settings(warn_only=True):
            run("supervisord -c ~/supervisord.conf")


def upload_key_to_larva():
    admin()
    with settings(warn_only=True):
        sudo("su -c \"echo 'ssh-rsa AAAAB3NzaC1yc2EAAAADAQABAAABAQDIG5QpeKt1uh0+yz17TIt3d1S9mV6ZnKXmK7DgtPofNoWg7z4Bi00BpDNLkjKGUMc/SZL9JUkscyb7yXoQXNip23Fdkxy4PPEHr3/6BdZ+7iwc2+5v+AfLsB7pbg/kdxfrGqhAZ9TFHKP8rjOUf8CR8fDUD1L5DNHK65yWF2iMt4fy+Awjibc2TMgphcy36ErSs83vETWrZXNzPhoAunRfD69ulluu6SWzfogypqaha7QRNnMWAOVzvTJYMAxVQ1h5GSZgCvaiqOkN5zUUVGEpKBfrZTsHsICyDpxphj9VA7hxH20wlIf4YlUvVRSngS97b10gTquTc4U84ZmVc1uT larva@axiompvd' > /home/larva/.ssh/authorized_keys\"")
        sudo("chmod 600 /home/larva/.ssh/authorized_keys")
        sudo("chmod 700 /home/larva/.ssh")


def setup_crontab():
    larva()
    src_file = "deploy/larva_crontab.txt"
    dst_file = "/home/larva/crontab.txt"
    upload_template(src_file, dst_file, context=copy(env), use_jinja=True, use_sudo=False, backup=False, mirror_local_mode=True)
    run("crontab %s" % dst_file)


# Usually this is all that needs to be called
def deploy():
    if env.roledefs['web']:
        execute(deploy_web)
    if env.roledefs['datasets']:
        execute(deploy_datasets)
    if env.roledefs['shorelines']:
        execute(deploy_shorelines)
    if env.roledefs['runs']:
        execute(deploy_runs)
    if env.roledefs['workers']:
        execute(deploy_workers)
    if env.roledefs['particles']:
        execute(deploy_particles)
    if env.roledefs['all']:
        execute(deploy_all)
