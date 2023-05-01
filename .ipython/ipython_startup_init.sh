#!/bin/bash

if [ -z ${IPYTHON_DIR+x} ]
then
    echo "IPYTHON_DIR is unset"
else
    echo "IPYTHON_DIR is set to '$IPYTHON_DIR'"
    if [[ $DB_IS_DRIVER = "TRUE" ]]; then
      ipython profile create default
      mkdir -p /root/.ipython/profile_default/startup
      rm -f /root/.ipython/profile_default/startup/*.py
      cp $IPYTHON_DIR/profile_default/startup/*.py /root/.ipython/profile_default/startup
    fi
fi