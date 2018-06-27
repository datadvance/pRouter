pRouter
=======

pRouter is crossplatform distributed job manager and HTTP proxy. It uses multiple
`pAgent <https://github.com/datadvance/pAgent>`_ instances to run jobs across multiple machines. Each job can be an
interactive HTTP/Websocket server providing its own administration API.

All features of pRouter are available through simple HTTP API.

It is intended to be used as a part of a larger software system that requires
executing distributed interactive jobs.

Configuration
-------------

pRouter can be configured from both command line and from YAML config file.

Command line description::

    pRouter - aggregate multiple pAgents.

    optional arguments:
    -h, --help            show this help message and exit
    --config CONFIG       config file
    --log-level {warning,debug,error,info,fatal} output log level
    --connection-debug    enable additional debug output for RPC connections
    --set SET             set config parameter, format is 'config_key=value',
                          values are interpreted as python literals, may appear
                          multiple times

    Config parameters:

        client.polling_delay - Delay (in seconds) between checking if active agent connection are idle and may be safely dropped.

        control.interface - Interface (ip address) to listen on.
        control.port - Port number to use. Can be set to 0 to use any free port.

        identity.name - Human-friendly router name (optional).
        identity.uid - Router's unique identifier. Arbitrary non-empty string.

        server.accept_tokens - List of valid client tokens.
        server.enabled - Enable router server (passive) mode so agents can connect as clients.
        server.interface - Interface (ip address) to listen on.
        server.port - Port number to use. Can be set to 0 to use any free port.


HTTP API
--------

pRouter binds 2 TCP endpoints - one is used for incoming connections from pAgent
while other exposes administrative functions and job API.

Available HTTP commands on administrative endpoint:

* GET '/info' - JSON containing basic info about the router (platform it is running on, its uid and name)

* GET '/connections' - JSON describing all active connections with pAgents.

* POST '/shutdown' - Initiate graceful exit.

* POST '/jobs/create' - Create a new (empty) job. Returns JSON object containing JOB_PATH. Select agent by uid::

    {
        'agent': {
            'type': 'uid',
            'uid': AGENT_UID
        },
        'name': JOB_NAME
    }

* POST '/jobs/create' - Create a new (empty) job. Returns JSON object containing JOB_PATH. Select agent by address::

    {
        'agent': {
            'type': 'address',
            'address': AGENT_ADDRESS_AND_PORT,
            'token': AGENT_ACCESS_TOKEN
        },
        'name': JOB_NAME
    }


* POST '/jobs/JOB_ID/start' - Start a job. Arguments::

    {
        'args': [ARGUMENT_LIST],
        'env': {ENVIRONMENT_DICT: ...}
        'name': JOB_NAME
    }

* POST '/jobs/JOB_PATH/wait' - wait for job to complete.

* GET '/jobs/JOB_PATH/info' - get job status.

* ANY '/jobs/JOB_PATH/http' - access job as an HTTP or Websocket server.

* GET '/jobs/JOB_PATH/file/RELATIVE_FILE_PATH' - download a file from job sandbox.

* POST (application/octet-stream) '/jobs/JOB_PATH/file/RELATIVE_FILE_PATH' - upload a file to job sandbox.

Contributing
------------

This project is developed and maintained by DATADVANCE LLC. Please
submit an issue if you have any questions or want to suggest an
improvement.

Acknowledgements
----------------

This work is supported by the Russian Foundation for Basic Research
(project No. 15-29-07043).
