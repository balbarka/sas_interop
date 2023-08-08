#!/bin/bash

if [ -z ${IPYTHON_PROFILE_DIR+x} ]
then
    echo "IPYTHON_PROFILE_DIR is unset"
else
    echo "IPYTHON_PROFILE_DIR is set to '$IPYTHON_PROFILE_DIR'"
    if [[ $DB_IS_DRIVER = "TRUE" ]]; then
      ipython profile create default
      mkdir -p /root/.ipython/profile_default/startup
      rm -f /root/.ipython/profile_default/startup/*.py
      cp $IPYTHON_PROFILE_DIR/startup/*.py /root/.ipython/profile_default/startup
    fi
fi