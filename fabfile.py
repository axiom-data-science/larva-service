from fabric.api import *
from fabric.contrib.files import *
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
"""

env.user = "larva"

code_dir = "/home/larva/larva-service"
env["code_dir"] = code_dir

env.roledefs.update({
    'setup'     : [],
    'web'       : [],
    'datasets'  : [],
    'shorelines': [],
    'runs'      : [],
    'workers'   : [],
    'all'       : ["calcium.axiompvd"]
})


# For copy and pasting when running tasks system wide
# @roles('web','datasets','shorelines','runs','workers','all')


def admin():
    env.user = "axiom"


def larva():
    env.user = "larva"


@roles('workers')
@parallel
def deploy_workers():
    stop_supervisord()
    larva()
    with cd(code_dir):
        update_code()
        update_libs()
        start_supervisord()
        run("supervisorctl -c ~/supervisord.conf start runs")
        run("supervisorctl -c ~/supervisord.conf start datasets")
        run("supervisorctl -c ~/supervisord.conf start shorelines")


@roles('runs')
@parallel
def deploy_runs():
    stop_supervisord()
    larva()
    with cd(code_dir):
        update_code()
        update_libs()
        start_supervisord()
        run("supervisorctl -c ~/supervisord.conf start runs")


@roles('datasets')
@parallel
def deploy_datasets():
    stop_supervisord()
    larva()
    with cd(code_dir):
        update_code()
        update_libs()
        start_supervisord()
        run("supervisorctl -c ~/supervisord.conf start datasets")


@roles('shorelines')
@parallel
def deploy_shorelines():
    stop_supervisord()
    larva()
    with cd(code_dir):
        update_code()
        update_libs()
        start_supervisord()
        run("supervisorctl -c ~/supervisord.conf start shorelines")


@roles('web')
@parallel
def deploy_web():
    stop_supervisord()
    larva()
    with cd(code_dir):
        update_code()
        update_libs()
        start_supervisord()
        run("supervisorctl -c ~/supervisord.conf start gunicorn")


@roles('all')
def deploy_all():
    stop_supervisord()
    larva()
    with cd(code_dir):
        update_code()
        update_libs()
        start_supervisord()
        run("supervisorctl -c ~/supervisord.conf start all")


@roles('setup')
@parallel
def setup_cloud_centos():
    # Based on Amazon Linux AMI
    admin()

    # Enable EPEL repo
    put(local_path='deploy/epel.repo', remote_path='/etc/yum.repos.d/epel.repo', use_sudo=True, mirror_local_mode=True)

    # Install additonal packages
    sudo("yum -y install proj-devel geos-devel git nginx python27 python27-devel gcc gcc-c++ make freetype-devel libpng-devel libtiff-devel libjpeg-devel")

    # Add /usr/local/lib to ld's path
    setup_ld()

    # Setup larva user
    setup_larva_user()

    # Setup the python virtualenv
    setup_burrito()

    # Install GDAL
    setup_gdal()

    # Get code
    setup_code()

    # Get NetCDF libraries for RedHat
    update_netcdf_libraries_rh()

    # Process requirements.txt
    install_requirements()

    # Setup Nginx
    execute(setup_nginx)

    # Data/Bathy (EBS from snapshot)
    setup_data()

    # Scratch Area (empty EBS)
    setup_scratch()

    # Setup Filesystem
    setup_filesystem()

    # Setup a Munin node
    setup_munin()

    # Crontab to remove old cache
    setup_crontab()

    # Setup supervisord
    update_supervisord()


@roles('setup')
def setup_local_debian():
     # Based on Debian Wheezy
    admin()

    # Install additonal packages
    sudo("apt-get install -y gfortran liblzo2-dev libbz2-dev libblas-dev liblapack-dev curl libgdal-dev libproj-dev libgeos-dev git nginx python2.7 python2.7-dev gcc g++ make libfreetype6-dev libpng-dev libtiff-dev libjpeg-dev")

    # Setup larva user
    setup_larva_user()

    # Setup the python virtualenv
    setup_burrito()

    # Get code
    setup_code()

    # Get NetCDF libraries for Debian
    update_netcdf_libraries_debian()

    # Process requirements.txt
    install_requirements()

    # Setup Nginx
    execute(setup_nginx)

    # Setup supervisord
    update_supervisord()


def setup_ld():
    admin()
    sudo("su -c \"echo '/usr/local/lib' > /etc/ld.so.conf.d/local.conf\"")
    sudo("ldconfig")


def setup_gdal():
    admin()
    run("cd ~")
    run("wget http://download.osgeo.org/gdal/gdal-1.9.2.tar.gz")
    run("tar zxvf gdal-1.9.2.tar.gz")
    with cd("gdal-1.9.2"):
        run("./configure; make -j 4")
        sudo("make install")


@roles('setup')
def setup_nginx():
    admin()
    upload_template('deploy/nginx.conf', '/etc/nginx/nginx.conf', context=copy(env), use_sudo=True, backup=False, mirror_local_mode=True)
    upload_template('deploy/nginx_larva.conf', '/etc/nginx/conf.d/larva.conf', context=copy(env), use_sudo=True, backup=False, mirror_local_mode=True)
    sudo("insserv nginx")
    sudo("/etc/init.d/nginx restart")


@roles('all')
def update_supervisord():
    larva()
    run("pip install supervisor")
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


@roles('all')
def install_requirements():
    larva()
    update_code()
    with cd("~"):
        # We do these seperately becasue they are Pytables requirements
        run("pip install numpy==1.8.0")
        run("pip install numexpr")
        run("pip install cython")

        # Pytables.  Ugh.  v.3.1.0
        run("rm -rf PyTables")
        run("git clone https://github.com/PyTables/PyTables.git")
        with cd("PyTables"):
            run("git checkout v.3.1.0")
            run('HDF5_DIR=/opt/hdf5-1.8.12 python setup.py install --hdf5=/opt/hdf5-1.8.12 --lflags="-Xlinker -rpath -Xlinker /opt/hdf5-1.8.12/lib" --cflags="-w -O3 -msse2"')

    with cd(code_dir):
        run("HDF5_DIR=/opt/hdf5-1.8.12 NETCDF4_DIR=/opt/netcdf-4.3.1 pip install netCDF4")
        run("pip install -e git+https://github.com/kwilcox/paegan.git@master#egg=paegan")
        run("CPLUS_INCLUDE_PATH=/usr/include/gdal C_INCLUDE_PATH=/usr/include/gdal pip install -e git+https://github.com/kwilcox/paegan-transport.git@master#egg=paegan-transport")
        run("pip install -e git+https://github.com/kwilcox/paegan-viz.git@master#egg=paegan-viz")
        run("pip install -r requirements.txt")


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


@roles('setup')
def setup_data():
    admin()
    with settings(warn_only=True):
        sudo("umount /data")
        sudo("umount /dev/sdf")
        sudo("mkdir /data")

    # Get instance's ID
    instance_id = run("wget -q -O - http://169.254.169.254/latest/meta-data/instance-id")
    # Get instance's availability zone
    zone = run("wget -q -O - http://169.254.169.254/latest/meta-data/placement/availability-zone")

    # Detach the current volume
    detach_vol_id = run("ec2-describe-instances %s --aws-access-key %s --aws-secret-key %s | awk '/\/dev\/sdf/ {print $3}'" % (instance_id, env.aws_access, env.aws_secret))
    if detach_vol_id.find("vol-") == 0:
        run("ec2-detach-volume %s --aws-access-key %s --aws-secret-key %s" % (detach_vol_id, env.aws_access, env.aws_secret))

    # Create new volume from snapshot
    volume = run("ec2-create-volume --aws-access-key %s --aws-secret-key %s --snapshot %s -z %s" % (env.aws_access, env.aws_secret, data_snapshot, zone))
    #volume = "VOLUME    vol-164df04f    20  snap-94f3cfd7   us-east-1c  creating    2013-04-17T19:40:05+0000    standard"
    vol_index = volume.find("vol-")
    volume_id = volume[vol_index:vol_index+12]

    # Wait for the old volume to be detached and new volume to be created
    time.sleep(30)
    sudo("ec2-attach-volume --aws-access-key %s --aws-secret-key %s -d /dev/sdf -i %s %s" % (env.aws_access, env.aws_secret, instance_id, volume_id))

    # Delete the old volume
    if detach_vol_id.find("vol-") == 0:
        run("ec2-delete-volume %s --aws-access-key %s --aws-secret-key %s" % (detach_vol_id, env.aws_access, env.aws_secret))


@roles('setup')
def setup_scratch():
    admin()
    with settings(warn_only=True):
        sudo("umount /scratch")
        sudo("umount /dev/sdg")
        sudo("mkdir /scratch")

    # Get instance's ID
    instance_id = run("wget -q -O - http://169.254.169.254/latest/meta-data/instance-id")
    # Get instance's availability zone
    zone = run("wget -q -O - http://169.254.169.254/latest/meta-data/placement/availability-zone")

    # Detach the current volume
    detach_vol_id = run("ec2-describe-instances %s --aws-access-key %s --aws-secret-key %s | awk '/\/dev\/sdg/ {print $3}'" % (instance_id, env.aws_access, env.aws_secret))
    if detach_vol_id.find("vol-") == 0:
        run("ec2-detach-volume %s --aws-access-key %s --aws-secret-key %s" % (detach_vol_id, env.aws_access, env.aws_secret))

    # Create new volume from snapshot
    volume = run("ec2-create-volume --aws-access-key %s --aws-secret-key %s --size 200 -z %s" % (env.aws_access, env.aws_secret, zone))
    #volume = "VOLUME    vol-164df04f    20  snap-94f3cfd7   us-east-1c  creating    2013-04-17T19:40:05+0000    standard"
    vol_index = volume.find("vol-")
    volume_id = volume[vol_index:vol_index+12]

    # Wait for the old volume to be detached and new volume to be created
    time.sleep(30)
    sudo("ec2-attach-volume --aws-access-key %s --aws-secret-key %s -d /dev/sdg -i %s %s" % (env.aws_access, env.aws_secret, instance_id, volume_id))
    time.sleep(30)

    # Delete the old volume
    if detach_vol_id.find("vol-") == 0:
        run("ec2-delete-volume %s --aws-access-key %s --aws-secret-key %s" % (detach_vol_id, env.aws_access, env.aws_secret))


@roles('setup')
def setup_filesystem():
    admin()
    with settings(warn_only=True):
        # Data is mounted at /dev/sdf
        sudo("mount /dev/sdf /data")
        sudo("chown -R larva:larva /data")

        # Scratch is mounted at /dev/sdg
        sudo("umount /scratch")
        sudo("umount /dev/sdg")
        sudo("mkdir /scratch")
        sudo("mkfs.ext4 /dev/sdg")
        sudo("mount /dev/sdg /scratch")
        sudo("mkdir -p /scratch/output")
        sudo("mkdir -p /scratch/cache")
        sudo("chown -R larva:larva /scratch")


def setup_crontab():
    larva()
    src_file = "deploy/larva_crontab.txt"
    dst_file = "/home/larva/crontab.txt"
    upload_template(src_file, dst_file, context=copy(env), use_jinja=True, use_sudo=False, backup=False, mirror_local_mode=True)
    run("crontab %s" % dst_file)


def setup_munin():
    admin()
    sudo("yum install -y munin-node")
    sudo("chkconfig munin-node on")
    run("echo \"allow ^107\.22\.197\.91$\" | sudo tee -a /etc/munin/munin-node.conf")
    run("echo \"allow ^10\.190\.178\.210$\" | sudo tee -a /etc/munin/munin-node.conf")
    sudo("/etc/init.d/munin-node restart")


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
    if env.roledefs['all']:
        execute(deploy_all)
