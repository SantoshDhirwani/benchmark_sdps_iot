"""
Simple example for a user-defined service.
"""

from core.services.coreservices import CoreService, ServiceMode
import os
import subprocess
from pathlib import Path

class FlinkClusterService(CoreService):
    """
    Custom CORE Service

    :var str name: name used as a unique ID for this service and is required, no spaces
    :var str group: allows you to group services within the GUI under a common name
    :var tuple executables: executables this service depends on to function, if executable is
        not on the path, service will not be loaded
    :var tuple dependencies: services that this service depends on for startup, tuple of service names
    :var tuple dirs: directories that this service will create within a node
    :var tuple configs: files that this service will generate, without a full path this file goes in
        the node's directory e.g. /tmp/pycore.12345/n1.conf/myfile
    :var tuple startup: commands used to start this service, any non-zero exit code will cause a failure
    :var tuple validate: commands used to validate that a service was started, any non-zero exit code
        will cause a failure
    :var ServiceMode validation_mode: validation mode, used to determine startup success.
        NON_BLOCKING    - runs startup commands, and validates success with validation commands
        BLOCKING        - runs startup commands, and validates success with the startup commands themselves
        TIMER           - runs startup commands, and validates success by waiting for "validation_timer" alone
    :var int validation_timer: time in seconds for a service to wait for validation, before determining
        success in TIMER/NON_BLOCKING modes.
    :var float validation_validation_period: period in seconds to wait before retrying validation,
        only used in NON_BLOCKING mode
    :var tuple shutdown: shutdown commands to stop this service
    """

    name = "Flink_Cluster"
    group = "BDAPRO"
    # executables = ()
    # dependencies = ()
    # dirs = ()
    configs = ()
    # startup = tuple(f"sh {x}" for x in configs)
    # validate = ()
    validation_mode = ServiceMode.NON_BLOCKING
    validation_timer = 5
    validation_period = 0.5
    # shutdown = ()

    @classmethod
    def on_load(cls):
        """
        Provides a way to run some arbitrary logic when the service is loaded, possibly to help facilitate
        dynamic settings for the environment.

        :return: nothing
        """
    # starts a flink cluster (Starts a Job Manager on the local node, connects via SSH to all worker nodes in slave to
    # start task manager on each node.
    FLINK_PATH = os.path.join(os.path.expanduser('~'),'Downloads','flink-1.10.1-bin-scala_2.11','flink-1.10.1')
    subprocess.run(os.path.join(FLINK_PATH,'bin/start-cluster.sh'), shell=True)

    #Only 1 node:
    # 1. config flink intallation for 1 node, tune the no. of cpu slots.
    # multi nodes:
    # 1. Decide which node runs Job Manager & which nodes run task managers.
    # 2. Ensure there's SSH connections between all nodes.
    # 3. config slave file in flink config files
    # 4. Run start-cluster on the node that runs the job manager(unique node/service)
    # 5. node that contains task manager (does nothing)
