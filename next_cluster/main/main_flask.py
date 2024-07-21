#!/storage/rhshui/lib/anaconda3/envs/web38/bin/python
# coding=utf-8
# © 2023 SHUI Ruihao <shuirh2019@gmail.com>
# All rights reserved.
"""
API of server monitor.
"""
import os
import sys
print('Add system path: {}'.format(os.getcwd()))
sys.path.append(os.getcwd())

import json
import toml
import re
from collections import OrderedDict
import time
import argparse

from flask import Flask, request, jsonify, make_response

from next_cluster.main.main_daemon import Cluster
from next_cluster.utils.teamup import translate_next

def main():
    parser = argparse.ArgumentParser(description='GPU Cluster Monitor API')
    parser.add_argument('--config', '-c', help = 'toml config file', 
                        default = 'config.toml')
    args = parser.parse_args()
    config = toml.load(args.config)['main']

    print(json.dumps(config, indent = 4))
    

    next_server = Cluster(
        config['host_data'],
        port = config.get('client_port'),
        passwd = config.get('passwd'),
        add_calendar = config['add_calendar'],
        teamup_ids = config.get('teamup_ids'),
        num_days = config.get('num_days'),
        name_translate = translate_next,
        node_wait = config.get('node_wait'),
        node_expire_time = config.get('node_expire_time'),
        cal_wait = config.get('cal_wait'), # calendar referesh interval
        dur_book_update = config.get('dur_book_update') # interval to refersh status
    )

    app = build_app(next_server)

    app.run(host = '0.0.0.0', port=config['port'], threaded = True)

#------------------------------
# Route
#------------------------------
def build_app(next_server):
    app = Flask(__name__)

    @app.route('/')
    def homepage():
        with open('monitor_home.html', encoding='utf8') as f:
            page = f.read()
        return page

    @app.route('/web/<fn>')
    def get_web(fn):
        with open('./web/{}'.format(fn), encoding='utf8') as f:
            s = f.read()
        r = make_response(s)
        if fn.split('.')[-1] == 'css':
            r.mimetype = 'text/css'
        elif fn.split('.')[-1] == 'js':
            r.mimetype = 'application/javascript'
        return r

    @app.route('/get-status', methods = ['GET'])
    def report_gpu_cluster():
        data = next_server.get_status()
        return jsonify(data)


    @app.route('/bookings', methods = ['GET'])
    def get_user_status():
        data = next_server.book_df.to_html()
        return data

    @app.route('/refresh-user', methods = ['GET'])
    def referesh_user():
        next_server.init_user_info()
        return 'Done'

    @app.route('/users', methods = ['GET'])
    def get_user():
        return '\n'.join(next_server._linux_users)
    
    return app

if __name__ == '__main__':
    main()
    
