#
# coding: utf-8
# Copyright (c) 2017 DATADVANCE
#
# Permission is hereby granted, free of charge, to any person obtaining a copy
# of this software and associated documentation files (the "Software"), to deal
# in the Software without restriction, including without limitation the rights
# to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
# copies of the Software, and to permit persons to whom the Software is
# furnished to do so, subject to the following conditions:
#
# The above copyright notice and this permission notice shall be included in all
# copies or substantial portions of the Software.
#
# THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS
# OR IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
# FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
# AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
# LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
# OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE
# SOFTWARE.
#


def nullable(schema):
    return {
        'anyOf': [
            {'type': 'null'},
            schema
        ]
    }


def fixed_object(properties):
    assert isinstance(properties, dict)
    return {
        'type': 'object',
        'properties': properties,
        'additionalProperties': False,
        'required': list(properties.keys())
    }


TOKEN = {
    'type': 'string',
    'minLength': 1,
    'description': 'Authentication token.'
}


IDENTITY = fixed_object(
    {
        'uid': nullable({
            'type': 'string',
            'description': (
                'Router\'s unique identifier. Arbitrary non-empty string.'
            )
        }),
        'name': {
            'type': 'string',
            'description': 'Human-friendly router name (optional).'
        }
    }
)


SERVER = fixed_object(
    {
        'enabled': {
            'type': 'boolean',
            'description': (
                'Enable router server (passive) '
                'mode so agents can connect as clients.'
            )
        },
        'port': {
            'type': 'integer',
            'description': (
                'Port number to use. Can be set to 0 to use any free port.'
            )
        },
        'interface': nullable({
            'type': 'string',
            'description': 'Interface (ip address) to listen on.'
        }),
        'accept_tokens': {
            'type': 'array',
            'items': TOKEN,
            'description': 'List of valid client tokens.'
        }
    }
)


CONTROL = fixed_object(
    {
        'port': {
            'type': 'integer',
            'description': (
                'Port number to use. Can be set to 0 to use any free port.'
            )
        },
        'interface': nullable({
            'type': 'string',
            'description': 'Interface (ip address) to listen on.'
        })
    }
)

CLIENT = fixed_object(
    {
        'polling_delay': {
            'type': 'number',
            'minValue': 0,
            'description': (
                'Delay (in seconds) between checking if active '
                'agent connection are idle and may be safely dropped.'
            )
        },
    }
)


CONFIG = fixed_object(
    {
        'client': CLIENT,
        'control': CONTROL,
        'identity': IDENTITY,
        'server': SERVER
    }
)
