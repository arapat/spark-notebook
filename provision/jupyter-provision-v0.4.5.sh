#!/bin/bash

# AWS EMR bootstrap script for installing Jupyter notebooks using Anaconda
# 2018-02-15 - Tested with EMR 5.11.1
# 2018-03-27 - Tested with EMR 5.12.0
# 2018-04-02 - Julaiti added Python 3.4
# 2018-04-05 - Tested with EMR 5.13.0
# 2020-04-08 - Have curl follow 30X redirects

ANACONDA_VERSION="5.0.1"
ANACONDA_PYTHON_VERSION="3"

PASSWORD=$1
PYTHON_VERSION=$2

# check for master node
if grep isMaster /mnt/var/lib/info/instance.json | grep true;
then
    # Download, install and set environment for Anaconda Python
    curl -L -o /mnt/tmp/anaconda.sh https://repo.continuum.io/archive/Anaconda$ANACONDA_PYTHON_VERSION-$ANACONDA_VERSION-Linux-x86_64.sh
    bash /mnt/tmp/anaconda.sh -b -p /mnt/anaconda
    echo '' >> /home/hadoop/.bashrc
    echo 'export PATH="/mnt/anaconda/bin:$PATH"' >> /home/hadoop/.bashrc
    echo 'export SPARK_HOME=/usr/lib/spark' >> /home/hadoop/.bashrc
    echo 'export PYTHONPATH=$SPARK_HOME/python/:$SPARK_HOME/python/lib/py4j-src.zip:$PYTHONPATH' >> /home/hadoop/.bashrc

    if [ ${PYTHON_VERSION} = "3" ];
    then
        echo 'export PYSPARK_PYTHON=/usr/bin/python3' >> /home/hadoop/.bashrc
        echo 'export PYSPARK_DRIVER_PYTHON=/usr/bin/python3' >> /home/hadoop/.bashrc
    fi

    source /home/hadoop/.bashrc
    python --version

    # Create the workspace and jupyter and ipython config directories
    mkdir -p /mnt/workspace
    mkdir -p ~/.jupyter
    mkdir -p ~/.ipython/profile_default

    # Generate the Jupyter notebook password
    NOTEBOOK_PASSWORD="$( bash <<EOF
python -c 'from notebook.auth import passwd
print(passwd("$PASSWORD"))'
EOF
    )"

    # Install yum packages
    sudo yum install git -y

    # Write the jupyter_notebook_config
    echo "c = get_config()" > ~/.jupyter/jupyter_notebook_config.py
    echo "c.NotebookApp.ip = '*'" >> ~/.jupyter/jupyter_notebook_config.py
    echo "c.NotebookApp.open_browser = False" >> ~/.jupyter/jupyter_notebook_config.py
    echo "c.NotebookApp.notebook_dir = '/mnt/workspace'" >> ~/.jupyter/jupyter_notebook_config.py
    echo "c.NotebookApp.password = u'$NOTEBOOK_PASSWORD'"  >> ~/.jupyter/jupyter_notebook_config.py

    # Write the ipython_config
    echo "c = get_config()" > ~/.ipython/profile_default/ipython_config.py
    echo 'c.InteractiveShellApp.exec_files = ["/home/hadoop/s3helper.py"]' >> ~/.ipython/profile_default/ipython_config.py

    # Download s3helper.py 
    curl -L -o /home/hadoop/s3helper.py https://raw.githubusercontent.com/mas-dse/spark-notebook/master/provision/remote/s3helper.py

    # Download the FileIO notebook to the workspace
    curl -L -o /mnt/workspace/FilesIO.ipynb https://raw.githubusercontent.com/mas-dse/spark-notebook/master/provision/workspace/FilesIO.ipynb

    # Install Python 2 kernel
    conda create -n ipykernel_py2 python=2 anaconda ipykernel -y
    source activate ipykernel_py2
    python -m ipykernel install --user
    source deactivate ipykernel_py2

    # Install Python 3.4 kernel
    conda create -n ipykernel_py34 python=3.4 anaconda ipykernel -y
    source activate ipykernel_py34
    python -m ipykernel install --user
    source deactivate

    # Install additional Python modules
    conda install -c conda-forge jupyter_nbextensions_configurator -y

    jupyter notebook &
fi
