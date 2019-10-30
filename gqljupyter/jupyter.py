#!/usr/bin/env python3

"""Install and start a GQL Jupyter kernel.

Usage: jupyter.py [--port=NNNN]

This script be run on a Mac or Linux desktop machine.  Your adhoc machine should
be running and have a valid grail-access ticket.

This script installs Jupyter notebook and a GQL kernel on the adhoc machine,
then starts the notebook server, then starts an SSH tunnel back to your
laptop/desktop. By default, it opens port 8888, but that can be changed by using
the '--port' flag.

Contact saito@grailbio.com if you find any problem.

For more details:
https://docs.google.com/document/d/1AM3fYMsfFgz0iD0Az35SZMUVXYFd5pbi393sh3oKei0/edit#heading=h.1z2ss3n75mw9

"""

import argparse
import logging
import os
import re
import shutil
import subprocess
import sys
import tempfile
import time
from concurrent.futures import ThreadPoolExecutor
from typing import List, NamedTuple

Config = NamedTuple('Config', [
    ('adhoc_addr', str),
    ('ticket_path', str)])

logging.basicConfig(level=logging.DEBUG)

# The directory on adhoc to install the grail-query binary.
INSTALL_DIR = '/home/ubuntu/.gql_install'

def check_call(args: List[str]) -> None:
    """Run a subprocess with the given argv."""
    logging.info('Run: %s', ' '.join(args))
    subprocess.check_call(args)

def check_output(args: List[str]) -> str:
    """Run a subprocess with the given argv and return its stdout."""
    logging.info('Run: %s', ' '.join(args))
    return subprocess.check_output(args, universal_newlines=True)

def install_jupyter_kernel() -> None:
    """Install the jupyter kernel config files. Jupyter must have been already installed.
    This function must run on adhoc"""
    from jupyter_client.kernelspec import KernelSpecManager

    dir_path = tempfile.mkdtemp()
    try:
        with open(os.path.join(dir_path, "kernel.json"), "w") as fd:
            fd.write("""
{"argv": ["%s/gqljupyter", "--jupyter-connection={connection_file}", "--log_dir=%s/log"], "display_name": "GQL", "language": "gql"}
""" % (INSTALL_DIR, INSTALL_DIR))
        with open(os.path.join(dir_path, "kernel.js"), "w") as fd:
            fd.write("""
// Kernel specific extension for lgo.
// http://jupyter-notebook.readthedocs.io/en/stable/extending/frontend_extensions.html#kernel-specific-extensions
define(function(){
  // var head = document.getElementsByTagName('head')[0];
  // var cssnode = document.createElement('link');
  // cssnode.type = 'text/css';
  // cssnode.rel = 'stylesheet';
  // cssnode.href = 'https://cdn.datatables.net/1.10.19/css/jquery.dataTables.css';
  // head.appendChild(cssnode);

  // var script = document.createElement('script');
  // script.src = 'https://code.jquery.com/jquery-1.12.4.js';
  // head.appendChild(script);

  // var script = document.createElement('script');
  // script.type = 'text/javascript';
  // script.charset = 'utf8';
  // script.src = 'https://cdn.datatables.net/1.10.15/js/jquery.dataTables.js';
  // head.appendChild(script);
  return {
    onload: function(){}
  }
});
""")
        KernelSpecManager().install_kernel_spec(dir_path, 'gql', user=True, replace=True)
    finally:
        shutil.rmtree(dir_path)

def get_adhoc_addr(adhoc_name: str) -> str:
    """Discover the IP address of the adhoc machine."""
    ipaddrs = []  #type: List[str]
    args = ['grail-adhoc', 'status']
    if adhoc_name:
        args.append('--name=' + adhoc_name)
    for line in check_output(args).split('\n'):
        line = line.strip()
        if not line:
            continue
        m = re.match(r'^i-\S+\s+(\S+)', line)
        assert m, "illegal grail-adhoc status output: " + line
        ipaddrs.append(m.group(1))
    if not ipaddrs:
        raise Exception("""No adhoc instance found. Run "grail-adhoc start". For
        more information, see https://phabricator.grailbio.com/w/docs/adhoc/""")
    if len(ipaddrs) > 1:
        ipaddrs.sort()
        logging.info('Multiple adhoc instances found (%s). Using %s', ipaddrs, ipaddrs[0])
    return ipaddrs[0]

def ssh(config: Config, argv: List[str]) -> None:
    """Run an ssh command on the adhoc machine."""
    check_call(['ssh', 'ubuntu@' + config.adhoc_addr] + argv)

def s3_cp(config: Config, src: str, dst: str) -> None:
    """Copy files src to dst. Either can be on S3."""
    check_call(['grail-aws', config.ticket_path, 's3', 'cp', src, dst])

def start_browser(port: int) -> None:
    """Start a browser on localhost:port."""
    time.sleep(1)
    url = 'http://localhost:%d' % (port, )
    if sys.platform.startswith('darwin'):
        check_call(['open', url])
    elif sys.platform.startswith('windows'):
        check_call(['cmd', '/c', 'start', url])
    else:
        check_call(['xdg-open', url])

def validate_gql_binary(config: Config) -> bool:
    """Check if gqljupyter binary on INSTALL_DIR is uptodate."""
    gql_path = INSTALL_DIR+'/gqljupyter'
    try:
        got = check_output(['ssh', 'ubuntu@' + config.adhoc_addr,
                            'sha256sum', gql_path]).split(' ')[0]
        want = check_output(['grail-aws', config.ticket_path,
                             's3', 'cp', 's3://grail-ysaito/gql/gqljupyter.sha256', '-']).split(' ')[0]
        if got == want:
            logging.info("You have the latest gqljupyter binary already")
            return True
        logging.error("%s on adhoc: sha256 mismatch (got %s <-> want %s)", gql_path, got, want)
    except Exception as e:
        logging.error("%s on adhoc: %s. Maybe you haven't installed jupyter yet? Try %s --install.",
                      gql_path, e, __file__)
    return False

def install_jupyter(config: Config) -> None:
    """Install jupyter notebook on an adhoc machine."""
    logging.info('Installing Jupyter and GQL')
    ssh(config, ['sudo', 'apt', 'install', 'python3', 'libzmq3-dev'])
    ssh(config, ['python3', '-m', 'pip', 'install', '--user', 'jupyter'])
    check_call(['scp', __file__, 'ubuntu@%s:/tmp/install_jupyter.py' % (config.adhoc_addr, )])
    ssh(config, ['python3', '/tmp/install_jupyter.py', '--install-adhoc-2'])

    ssh(config, ['mkdir', '-p', INSTALL_DIR])
    s3_cp(config, 's3://grail-ysaito/gql/gqljupyter', '/tmp/gqljupyter')
    subprocess.call(['ssh', 'ubuntu@' + config.adhoc_addr,
                     'killall', '-9', 'gqljupyter', 'jupyter-notebook'])
    check_call(['scp', '/tmp/gqljupyter', 'ubuntu@%s:%s/gqljupyter' % (config.adhoc_addr, INSTALL_DIR)])
    ssh(config, ['chmod', '755', INSTALL_DIR + '/gqljupyter'])

def main() -> None:
    """Main application entry point."""
    parser = argparse.ArgumentParser()
    parser.add_argument('--ticket-path', default='tickets/eng/dev/aws', type=str,
                        help="Vanadium ticket used to access Grail resources.")
    parser.add_argument('--install', action='store_true')
    parser.add_argument('--install-adhoc-2', action='store_true')
    parser.add_argument('--adhoc-name', default='', type=str,
                        help="Name of the adhoc instance (may be a glob pattern)")
    parser.add_argument('--release', default='', type=str, help="""Copy gqljupyter and
    jupyter.py to S3. The arg is the local path of the gqljupyter binary""")
    parser.add_argument('--port', default=8888, type=int)
    args = parser.parse_args()
    if args.install_adhoc_2:
        install_jupyter_kernel()
        sys.exit(0)

    config = Config(adhoc_addr=get_adhoc_addr(args.adhoc_name),
                    ticket_path=args.ticket_path)
    if args.install:
        install_jupyter(config)
        sys.exit(0)
    if args.release:
        if os.environ.get('USER') != 'ysaito':
            raise Exception('--release should be done only by ysaito')
        sha256_got = check_output(['sha256sum', args.release])
        with tempfile.NamedTemporaryFile(mode='w') as fd:
            fd.write(sha256_got)
            fd.flush()
            s3_cp(config, fd.name, 's3://grail-ysaito/gql/gqljupyter.sha256')
            s3_cp(config, args.release, 's3://grail-ysaito/gql/gqljupyter')
            s3_cp(config, __file__, 's3://grail-ysaito/gql/jupyter.py')
        sys.exit(0)

    if os.environ.get('USER') == 'ubuntu':
        logging.error('This script must run on your desktop/laptop, not adhoc')
        sys.exit(1)

    if not validate_gql_binary(config):
        install_jupyter(config)
        if not validate_gql_binary(config):
            raise Exception("Failed to install gql")

    subprocess.call(
        ['ssh', 'ubuntu@' + config.adhoc_addr, "ps awx | grep jupyter | awk '{print $1}' | xargs kill -9"])

    pool = ThreadPoolExecutor(max_workers=128)
    r = pool.submit(start_browser, args.port)
    logging.info('Starting jupyter notebook at localhost:%s', args.port)
    subprocess.check_call([
        'ssh',
        '-L', '%d:127.0.0.1:%d' % (args.port, args.port),
        'ubuntu@' + config.adhoc_addr,
        'export V23_CREDENTIALS=/home/ubuntu/.v23; ',
        '/home/ubuntu/.local/bin/jupyter',
        'notebook',
        '--ip=127.0.0.1',
        '--port=%d' % (args.port,),
        '--no-browser',
        '--NotebookApp.token=',
        '--NotebookApp.password='])

main()
