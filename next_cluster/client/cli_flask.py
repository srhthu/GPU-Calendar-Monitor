"""Build the flask app to expose node status via a port"""
from argparse import ArgumentParser
import toml
import logging
from pathlib import Path
import json

from flask import Flask, request, jsonify, make_response, abort
from flask.logging import default_handler

from next_cluster.client.client_daemon import NodeStat

def build_app(node: NodeStat, passwd):
    app = Flask(__name__)

    @app.route('/get-status', methods = ['POST'])
    def node_status():
        pw = request.json.get('passwd', None)
        if passwd is None or pw == passwd:
            return jsonify(node.status)
        else:
            abort(404)
    
    @app.route('/', methods = ['GET'])
    def home():
        pw = request.args.get('passwd')
        if passwd is None or pw == passwd:
            return json.dumps(node.status, indent = 4, ensure_ascii= False)
        else:
            abort(404)
    
    return app

def main():
    parser = ArgumentParser(description='REST API to expose node status')
    # config file. Recommended
    parser.add_argument('--config', '-c', help = 'config file path', 
                        default = 'config.toml')
    # can also pass arguments via command line arguments
    parser.add_argument('--interval', type = int, default=None)
    parser.add_argument('--interval_proc', type = int, default=None)
    parser.add_argument('--extra_keys', nargs = '+', default=None)
    parser.add_argument('--port', type = int, default = None,
                        help = 'Port to access node status. (ip:port/get-status)')
    parser.add_argument('--passwd', 
                        help = ('password to access the node status. '
                                'If set to None, then no password is required'))

    args = parser.parse_args()

    # Load config
    if args.config is not None and Path(args.config).exists():
        print(f'Load config file from {args.config}')
        config = toml.load(args.config)['client']
    else:
        config = {}
    
    # overwrite cmd args
    node_keys = ['interval', 'interval_proc', 'extra_keys']
    all_keys = node_keys + ['port', 'passwd']

    for key in all_keys:
        v = args.__dict__[key]
        if v is not None:
            print(f'Overwrite config key: {key}={v}')
            config[key] = v
    if 'passwd' not in config:
        config['passwd'] = None

    # Initialize node_stat
    node_cfg = {k:config[k] for k in node_keys if k in config}
    n_stat = NodeStat(**node_cfg)
    n_stat.start()

    # Build flask app
    app = build_app(n_stat, config['passwd'])
    log = logging.getLogger('werkzeug')
    log.setLevel(logging.WARNING)
    app.run(host = '0.0.0.0', port = args.port, threaded = True)

if __name__ == '__main__':
    main()