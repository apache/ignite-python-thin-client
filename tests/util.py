# Licensed to the Apache Software Foundation (ASF) under one or more
# contributor license agreements.  See the NOTICE file distributed with
# this work for additional information regarding copyright ownership.
# The ASF licenses this file to You under the Apache License, Version 2.0
# (the "License"); you may not use this file except in compliance with
# the License.  You may obtain a copy of the License at
#
#      http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
import asyncio
import contextlib
import glob
import inspect
import os
import shutil

import jinja2 as jinja2
import psutil
import re
import signal
import subprocess
import time


@contextlib.contextmanager
def get_or_create_cache(client, settings):
    cache = client.get_or_create_cache(settings)
    try:
        yield cache
    finally:
        cache.destroy()


@contextlib.asynccontextmanager
async def get_or_create_cache_async(client, settings):
    cache = await client.get_or_create_cache(settings)
    try:
        yield cache
    finally:
        await cache.destroy()


def wait_for_condition(condition, interval=0.1, timeout=10, error=None):
    start = time.time()
    res = condition()

    while not res and time.time() - start < timeout:
        time.sleep(interval)
        res = condition()

    if res:
        return True

    if error is not None:
        raise Exception(error)

    return False


async def wait_for_condition_async(condition, interval=0.1, timeout=10, error=None):
    start = time.time()
    res = await condition() if inspect.iscoroutinefunction(condition) else condition()

    while not res and time.time() - start < timeout:
        await asyncio.sleep(interval)
        res = await condition() if inspect.iscoroutinefunction(condition) else condition()

    if res:
        return True

    if error is not None:
        raise Exception(error)

    return False


def is_windows():
    return os.name == "nt"


def get_test_dir():
    return os.path.dirname(os.path.realpath(__file__))


def get_ignite_dirs():
    ignite_home = os.getenv("IGNITE_HOME")
    if ignite_home is not None:
        yield ignite_home

    proj_dir = os.path.abspath(os.path.join(get_test_dir(), "..", ".."))
    yield os.path.join(proj_dir, "ignite")
    yield os.path.join(proj_dir, "incubator_ignite")


def get_ignite_runner():
    ext = ".bat" if is_windows() else ".sh"
    for ignite_dir in get_ignite_dirs():
        runner = os.path.join(ignite_dir, "bin", "ignite" + ext)
        print("Probing Ignite runner at '{0}'...".format(runner))
        if os.path.exists(runner):
            return runner

    raise Exception(f"Ignite not found. IGNITE_HOME {os.getenv('IGNITE_HOME')}")


def check_server_started(idx=1):
    pattern = re.compile('^Topology snapshot.*')

    for log_file in get_log_files(idx):
        with open(log_file) as f:
            for line in f.readlines():
                if pattern.match(line):
                    return True

    return False


def kill_process_tree(pid):
    if is_windows():
        subprocess.call(['taskkill', '/F', '/T', '/PID', str(pid)])
    else:
        children = psutil.Process(pid).children(recursive=True)
        for child in children:
            os.kill(child.pid, signal.SIGKILL)
        os.kill(pid, signal.SIGKILL)


templateLoader = jinja2.FileSystemLoader(searchpath=os.path.join(get_test_dir(), "config"))
templateEnv = jinja2.Environment(loader=templateLoader)


def create_config_file(tpl_name, file_name, **kwargs):
    template = templateEnv.get_template(tpl_name)
    with open(os.path.join(get_test_dir(), "config", file_name), mode='w') as f:
        f.write(template.render(**kwargs))


def start_ignite(idx=1, debug=False, use_ssl=False, use_auth=False, use_persistence=False):
    clear_logs(idx)

    runner = get_ignite_runner()

    env = os.environ.copy()

    if debug:
        env["JVM_OPTS"] = "-Djava.net.preferIPv4Stack=true -Xdebug -Xnoagent -Djava.compiler=NONE " \
                          "-agentlib:jdwp=transport=dt_socket,server=y,suspend=n,address=5005 "

    if use_auth:
        use_persistence = True

    params = {
        'ignite_instance_idx': str(idx),
        'ignite_client_port': 10800 + idx,
        'use_ssl': use_ssl,
        'use_auth': use_auth,
        'use_persistence': use_persistence,
    }

    create_config_file('log4j.xml.jinja2', f'log4j-{idx}.xml', **params)
    create_config_file('ignite-config.xml.jinja2', f'ignite-config-{idx}.xml', **params)

    ignite_cmd = [runner, os.path.join(get_test_dir(), "config", f'ignite-config-{idx}.xml')]
    print("Starting Ignite server node:", ignite_cmd)

    srv = subprocess.Popen(ignite_cmd, env=env, cwd=get_test_dir())

    started = wait_for_condition(lambda: check_server_started(idx), timeout=60)
    if started:
        return srv

    kill_process_tree(srv.pid)
    raise Exception("Failed to start Ignite: timeout while trying to connect")


def start_ignite_gen(idx=1, use_ssl=False, use_auth=False, use_persistence=False):
    srv = start_ignite(idx, use_ssl=use_ssl, use_auth=use_auth, use_persistence=use_persistence)
    try:
        yield srv
    finally:
        kill_process_tree(srv.pid)


def get_log_files(idx=1):
    logs_pattern = os.path.join(get_test_dir(), "logs", "ignite-log-{0}*.txt".format(idx))
    return glob.glob(logs_pattern)


def clear_ignite_work_dir():
    for ignite_dir in get_ignite_dirs():
        work_dir = os.path.join(ignite_dir, 'work')
        if os.path.exists(work_dir):
            shutil.rmtree(work_dir, ignore_errors=True)


def clear_logs(idx=1):
    for f in get_log_files(idx):
        os.remove(f)
