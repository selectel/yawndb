YAWNDB
======

[![Build Status](https://travis-ci.org/selectel/yawndb.svg?branch=master)](https://travis-ci.org/selectel/yawndb)

YAWNDB is an in-memory circular array database written in Erlang.  This language is
based upon the lightweight process model allowing to process large amounts of
data without consuming extra system resources.  In YAWNDB the data are stored
in a circular buffer named a conveyor in YAWNDB terminology.  All the incoming
data are dispersed and put into the archives named buckets according to certain
rules.  A rule is a set of properties for a set of statistical data (i.e. data
size, data collection frequency, etc.)

The data in YAWNDB are represented as triplets composed of time, value and key.
The key (in YAWNDB terminology it is also called path) is a sequence of
lowercase letters and numeric symbols defining on which conveyor the triplet
will be put.

Our approach allows to gain the following benefits:

* the obsolete data are deleted without excessive consumption of resources;
* fixed memory usage for a fixed number of keys;
* fixed access time for any records.

Architecture
------------

YAWNDB is based on a circular array algorithm implemented in Ecirca library
written in C.  YAWNDB modules written in Erlang interact with Ecirca via native
implemented functions (NIF).  Data retention is provided by Bitcask application
written in Erlang.  The REST API is based on Cowboy web server.  The data is
written via the socket and read via the REST API interface.

Installation
------------

Clone the repository:

```
$ git clone https://github.com/selectel/yawndb.git
```

and then execute the following commands:

```
$ cd yawndb
$ make all
```

then

```
$ ./start.sh
```

Copy the configuration file:

```
cp priv/yawndb.yml.example priv/yawndb.yml
```

or create an appropriate symlink.

Configuration
-----------

All YAWNDB settings are kept in the configuration file yawndb.yml.

#### Data Saving Settings

| Parameter            | Type    | Description                                                                        |
|----------------------|---------|------------------------------------------------------------------------------------|
| flush_enabled        | boolean | Enabling/disabling saving to disk                                                  |
| flush_dir            | string  | Directory to save data                                                             |
| flush_period         | integer | Data saving frequency (in seconds)                                                 |
| flush_late_threshold | float   | Write to log if saving takes more than flush_period * flush_late_threshold seconds |
| flush_cache          | integer | Read cache                                                                         |
| flush_write_buffer   | integer | Write cache                                                                        |
| flush_block_size     | integer | Block size for saving to disk                                                      |


#### Obsolete data deletion settings

| Parameter            | Type    | Description                           |
|----------------------|---------|---------------------------------------|
| cleanup_enabled      | boolean | Enable/disable obsolete data deletion |
| cleanup_period       | integer | Deletion frequency, in seconds        |

#### User API settings

| Parameter            | Type    | Description                                             |
|----------------------|---------|---------------------------------------------------------|
| listen_jsonapi_req   | boolean | Enable/disable the user API                             |
| jsonapi_iface        | string  | Specify which network interface that will be used       |
| jsonapi_port         | integer | Specify which local port number to use                  |
| jsonapi_prespawned   | integer | Number of workers previously created (can be increased) |

#### Administrative API settings

| Parameter            | Type    | Description                                             |
|----------------------|---------|---------------------------------------------------------|
| listen_tcpapi_req    | boolean | Enabling/disabling the administrator API                |
| tcpapi_iface         | string  | Specify which network interface that will be used       |
| tcpapi_port          | integer | Specify which local port number to use                  |
| tcpapi_prespawned    | integer | Number of workers previously created (can be increased) |

#### Data storing settings

| Parameter            | Type    | Description                         |
|----------------------|---------|-------------------------------------|
| max_slice            | integer | Maximum size of the slice to choose |
| rules                | list    | Rules regulating data retention     |

#### Data storing rules

| Parameter            | Type    | Description                           |
|----------------------|---------|---------------------------------------|
| name                 | string  | The name of the rule                  |
| prefix               | string  | The prefix referred by the rule       |
| limit                | integer | Conveyor size                         |
| split                | string  | Define the way the value will be divided in case it does not match the bucket: proportional - the value will be proportionally divided between the two nearest buckets; equal - the value will be divided into halves between the two nearest buckets; forward/backward - the whole value wil be put into the previous (or the nearest) bucket |
| type                 | string  | Define the value to be saved in buckets: last - only the last value will be saved; max - the maximal incoming value will be saved; min - the minimal incoming value will be saved; sum - the sum of the incoming values will be saved; avg - the average value will be saved |
| value_size           | integer | Define the value size for a bucket (16, 32 и 64 bits), possible values: small, medium, large 
| additional_values    | list    | A list of "foobar: weak/strong" pairs from 0 to 14 symbols in length.  Sometimes it is necessary to save the additional values not being numbers ('timeout', for example).  The list of additional values is used for this purpose.  The values can be either strong or weak.  The weak values are updated if the bucket is updated while the strong values are never updated. |
| autoremove           | boolean | Enable/disable conveyor removal in case of data deterioration |

Get data API
---------------

The data are captured with the HTTP API.  There are two types of API in YAWNDB
- the administrative API and the user API.  All API settings are specified in
the configuration file in the sections Admin JSON API and User JSON API.

A typical API response is given in the following form:

```json
{
	"status": "ok",
	"code": "ok",
	"answer": {}
}
```

The status field may take the values `ok` or `error`.
The code field contains "ok" on success.  The answer is displayed in the answer
field.  If an error occurs, its code will be displayed in the code field; the
human-readable description of the error will be displayed in the answer field.

#### Get a list of rules for a selected path

`GET /paths/:path/rules`

Example of an answer:

```json
{
 "status": "ok",
 "code": "ok",
 "answer": ["stat"]
}
```

#### Get a list of time-value pairs for a given period:

`POST /paths/slice`

The conveyors are indicated in the body of the request: ```paths=path1/rule,path2/rule2,path3/rule3```.

Request parameters:

| Parameter | Type    | Description                 |
|-----------|---------|-----------------------------|
| from      | integer | Initial time, UTC timestamp |
| to        | integer | End time, UTC timestamp     |

Example of an answer:

```json
{
 "status": "ok",
 "code": "ok",
 "answer": [
	[63555098820, 10, 12],
	[63555098888, 16, "empty"],
	[63555098940, 12, 10],
	[63555099000, 3, 8],
	[63555099060, 10, 12],
	[63555099120, 2, 9],
	[63555099180, 7, 12],
	[63555099240, 3, 4],
	[63555099300, 20, 10],
	[63555099360, 2, 11]
 ]
}
```

#### Get a list of the last time-value pairs for a given period:

`GET /paths/:path/:rule/slice`

Request parameters:

| Parameter | Type    | Description                |
|-----------|---------|----------------------------|
| from      | integer | Initial time UTC timestamp |
| to        | integer | End time, UTC timestamp    |

Example of an answer:

```json
{
 "status": "ok",
 "code": "ok",
 "answer": [
	[63555098820, 12],
	[63555098880, "empty"],
	[63555098940, 10],
	[63555099000, "empty"],
	[63555099060, 15],
	[63555099120, "empty"],
	[63555099180, 12],
	[63555099240, "empty"],
	[63555099300, 10],
	[63555099360, "empty"]
 ]
}
```

The answer contains a list of time-value pairs for a given period; if there is
no value for a certain moment, the string `empty` is returned.

#### Get a list of the last time-value pairs for a conveyor:

Request parameters:

| Parameter | Type    | Description               |
|-----------|---------|---------------------------|
| n         | integer | The number of last values |

Example of an answer:


```json
{
 "status": "ok",
 "code": "ok",
 "answer": [
	[63555103140, 11],
	[63555103200, 10],
	[63555103260, 10],
	[63555103320, 12],
	[63555103380, 10],
	[63555103440, 10],
	[63555103500, 11],
	[63555103560, 10],
	[63555103620, 10],
	[63555103680, 14]
 ]
}
```
The answer contains a list of time-value pairs for a given period; if there is
no value for a certain moment, the string `empty` is returned.

Administrative API
------------------

#### Get a server status:

`GET /status`

Example of an answer:

```json
{
 "status": "ok",
 "code": "ok",
 "answer": {
	"read_rpm": 37,
	"write_rpm": 816042,
	"read_rps": 1,
	"write_rps": 13601,
	"processes_now": 162836,
	"processes_max": 300000,
	"paths_count": 162256,
	"dirty_paths_count": 19864
 }
}
```

Answer fields:

| Field             | Type    | Description                                  |
|-------------------|---------|----------------------------------------------|
| read_rpm          | integer | Read requests per minute                     |
| write_rpm         | integer | Write requests per minute                    |
| read_rps          | integer | Read requests per second                     |
| write_rps         | integer | Write requests per second                    |
| processes_now     | integer | Number of active processes                   |
| processes_max     | integer | Maximal number of active processes permitted |
| paths_count       | integer | Number of paths                              |
| dirty_paths_count | integer | Number of the unsaved paths                  |


#### Get the list of all paths saved

`GET /paths/all`

Example of an answer:

```json
{
 "status": "ok",
 "code": "ok",
 "answer": [
	"some-stats-a",
	"some-stats-b"
 ]
}
```

#### Delete a path

`DELETE /paths/:path`

Example of an answer:

```json
{
 "status": "ok",
 "code": "ok",
 "answer": "deleted"
}
```

#### Get an aggregated list of time-value pairs

`POST /aggregate`

The paths are indicated in the body of the request: ```paths=path1,path2,path3```.

Parameters of the request

| Parameter | Type    | Description                               |
|-----------|---------|-------------------------------------------|
| from      | integer | Initial time, UTC timestamp               |
| to        | integer | Ending time, UTC timestamp                |
| rule      | string  | The rule used                             |
| aggregate | string  | Aggregation function (max, min, avg, sum) |

Example of an answer:

```json
{
 "status": "ok",
 "code": "ok",
 "answer": [
	[63555096900, 12],
	[63555096960, "empty"],
	[63555097020, 10],
	[63555097080, 16],
	[63555097140, 10],
	[63555097200, "empty"],
	[63555097260, 11],
	[63555097320, 17],
	[63555097380,16],
	[63555097440, "empty"]
 ]
}
```

### Error codes

| Error          | HTTP-code | Description                                                                        |
|----------------|-----------|------------------------------------------------------------------------------------|
| no_aggregate   | 400       | The aggregate request parameter is not indicated or improperly formulated          |
| no_path        | 400       | The path request parameter is not indicated or improperly formulated               |
| no_rule        | 400       | The rule request parameter is not indicated or improperly formulated               |
| no_from        | 400       | The from request parameter is not indicated or improperly formulated               |
| no_to          | 400       | The to request parameter is not indicated or improperly formulated                 |
| no_n           | 400       | The n request parameter is not indicated or improperly formulated                  |
| no_paths       | 400       | The paths request parameter is not indicated or improperly formulated              |
| no_body        | 400       | The request body is expected but absent                                            |
| no_fun         | 400       | Unknown request                                                                    |
| from_to_order  | 400       | The value of the from parameter is greater than that of the to parameter           |
| page_not_found | 404       | Requested path not found                                                           |
| rule_not_found | 404       | The requested rule for the given path not found                                    |
| slice_too_big  | 413       | The number of request results is greater than the value of the max_slice parameter |
| process_limit  | 500       | Limit of active processes achieved                                                 |
| memory_limit   | 500       | Write limit achieved                                                               |
| internal       | 500       | Internal error                                                                     |

Save data API
---------

This API is used for saving new data to YAWNDB.  To save the data connect to
the TCP port indicated in the configuration file (TCP API options section) and
send the packets in the indicated format.  No reply from the server is expected.

#### The main types of data used

| Type   | Description                          |
|--------|--------------------------------------|
| uint8  | Unsigned 8-bit number                |
| uint16 | Unsigned 16-bit number in big-endian |
| uint64 | Unsigned 64-bit number in big-endian |
| byte*  | Byte sequence                        |

#### Packet structure

| Field  | Value                                                         |
|--------|---------------------------------------------------------------|
| uint16 | Packet size (except this field)                               |
| uint8  | Protocole version                                             |
| uint8  | Flag indicating special values (0 - the value is not special) |
| unit64 | Number of seconds from the beginning of UNIX era              |
| unit64 | Value (or the number of the special value)                    |
| byte*  | Byte sequence                                                 |

#### Constructing a packet in Python

```python
def encode_yawndb_packet(is_special, path, time, value):
    """
    Construct a packet to be sent to yawndb.

    :param bool is_special: special value? used for additional_values
    :param str path: statistics identifier
    :param int value: statistics value
    """
    is_special_int = 1 if is_special else 0
    pck_tail = struct.pack(">BBQQ", YAWNDB_PROTOCOL_VERSION, is_special_int,
                           time, value) + path
    pck_head = struct.pack(">H", len(pck_tail))
    return pck_head + pck_tail
```

#### Constructing a packet in C

```c
// indicate the protocol version
#define YAWNDB_PROTOCOL_VERSION 3

// describe the packet structure
struct yawndb_packet_struct {
	uint16_t	length;
	uint8_t 	version;
	int8_t  	isSpecial;
	uint64_t	timestamp;
	uint64_t	value;
	char    	path[];
};

// construct the packet
yawndb_packet_struct *encode_yawndb_packet(int8_t isSpecial,
	uint64_t timestamp, uint64_t value, const char * path)
{
	yawndb_packet_struct *packet;
	uint16_t length;

	length = sizeof(uint8_t) + sizeof(int8_t) + sizeof(uint64_t) + sizeof(uint64_t) + strlen(path);
	packet = malloc(length + sizeof(uint16_t));
	packet->length = htobe16(length);
	packet->version = YAWNDB_PROTOCOL_VERSION;
	packet->isSpecial = isSpecial;
	packet->timestamp = htobe64(timestamp);
	packet->value = htobe64(value);
	strncpy(packet->path, path, strlen(path));

	return packet;
}
```

Authors
-------

YAWNDB was written by [Dmitry Groshev](https://github.com/si14/) in 2012.
[Alexander Neganov](https://github.com/ikkeps),
[Kagami Hiiragi](https://github.com/kagami),
[Maxim Mitroshin](https://github.com/rocco66),
[Pavel Abalihin](https://github.com/tnt-dev) and
[Fedor Gogolev](https://github.com/knsd) have also contributed to the project.
