#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""
Licensed to the Apache Software Foundation (ASF) under one
or more contributor license agreements.  See the NOTICE file
distributed with this work for additional information
regarding copyright ownership.  The ASF licenses this file
to you under the Apache License, Version 2.0 (the
"License"); you may not use this file except in compliance
with the License.  You may obtain a copy of the License at
    http://www.apache.org/licenses/LICENSE-2.0
Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
"""

#Based on https://github.com/monolive/ambari-custom-alerts

import urllib2
#import ambari_simplejson as json # simplejson is much faster comparing to Python 2.6 json module and has the same functions set.
import json
import logging
from subprocess import *
import os
import time
from threading import Timer
import socket


from resource_management.libraries.functions.curl_krb_request import curl_krb_request
from resource_management.core.environment import Environment

LISTENERS = "{{kafka-broker/listeners}}"

TOPIC_NAME = 'topicName'


logger = logging.getLogger()

def get_tokens():
  """
  Returns a tuple of tokens in the format {{site/property}} that will be used
  to build the dictionary passed into execute
  """
  return (LISTENERS,)


def execute(configurations={}, parameters={}, host_name=None):
  """
  Returns a tuple containing the result code and a pre-formatted result label
  Keyword arguments:
  configurations (dictionary): a mapping of configuration key to value
  parameters (dictionary): a mapping of script parameter key to value
  host_name (string): the name of this host where the alert is running
  """

  if TOPIC_NAME not in parameters:
    return (('UNKNOWN', ['Parameter "%s" is missing'%TOPIC_NAME]))

  result_code="OK"
  label="Success : Publishing & consumption are OK"

  try:
    port=configurations[LISTENERS].split(":")[2]
    message = "%s : %s"  % (socket.getfqdn(), time.strftime("%Y/%m/%d %H:%M:%S", time.localtime()))

    cmd ="export KRB5CCNAME=/tmp/krb5cc_ambari_alerts_kafka\n"
    cmd+="kinit -kt /etc/security/keytabs/kafka.service.keytab kafka/$(hostname)\n"
    p=Popen(cmd, shell=True, stdout=PIPE, stderr=PIPE, stdin=PIPE)
    stdout, stderr = p.communicate(message+"\n")
    if p.returncode or "ERROR" in stderr or "Exception" in stderr:
      raise AssertionError("Can't execute kinit\nret=%s\nstdout=%s\nstderr=%s" % (p.returncode, stdout, stderr))

    cmd ="export KRB5CCNAME=/tmp/krb5cc_ambari_alerts_kafka\n"
    cmd+="/usr/hdp/current/kafka-broker/bin/kafka-console-producer.sh --broker-list %s:%s --topic %s --security-protocol PLAINTEXTSASL\n" % (socket.getfqdn(), port ,parameters[TOPIC_NAME])
    p=Popen(cmd, shell=True, stdout=PIPE, stderr=PIPE, stdin=PIPE)
    stdout, stderr = p.communicate(message+"\n")

    if p.returncode or "ERROR" in stderr or "Exception" in stderr:
      raise AssertionError("Can't push a message in topic '%s'\nret=%s\nstdout=%s\nstderr=%s" % (parameters[TOPIC_NAME], p.returncode, stdout, stderr))


    cmd ="export KRB5CCNAME=/tmp/krb5cc_ambari_alerts_kafka\n"
    #cmd+="/usr/hdp/current/kafka-broker/bin/kafka-simple-consumer-shell.sh --broker-list %s:%s --topic %s --security-protocol PLAINTEXTSASL --no-wait-at-logend --partition 0 | grep '%s'\n" % (socket.getfqdn(), port ,parameters[TOPIC_NAME], message)
    cmd+="echo $(timeout 2 /usr/hdp/current/kafka-broker/bin/kafka-console-consumer.sh --bootstrap-server %s:%s --topic %s --security-protocol PLAINTEXTSASL --from-beginning) | grep -s '%s'\n" % (socket.getfqdn, port ,parameters[TOPIC_NAME], message)
    cmd+="exit ${PIPESTATUS[0]}\n"
    p=Popen(cmd, shell=True, stdout=PIPE, stderr=PIPE, stdin=PIPE)
    stdout, stderr = p.communicate(message)
    if p.returncode or "ERROR" in stderr or "Exception" in stderr:
      raise AssertionError("Can't consume a message in topic '%s'\nret=%s\nstdout=%s\nstderr=%s" % (parameters[TOPIC_NAME],p.returncode, stdout, stderr))

    if stdout=="":
      raise AssertionError("Can't find the last message '%s' in topic '%s'\nret=%s\nstdout=%s\nstderr=%s" % (message, parameters[TOPIC_NAME],p.returncode, stdout, stderr))

  except AssertionError, e:
    label = str(e)
    result_code = "CRITICAL"
  except Exception, e:
    label = str(e)
    result_code = 'UNKNOWN'
  finally:
    try:
      os.remove("/tmp/krb5cc_ambari_alerts_kafka")
    except:
      pass

  return ((result_code, [label ]))
