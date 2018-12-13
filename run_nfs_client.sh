#!/usr/bin/env bash
pkill -f nfs_nlm_wrapper.py
source env/bin/activate
python nfs_nlm_wrapper.py&