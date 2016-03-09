#!/usr/bin/python
# Copyright 2012 William Yu
# wyu@ateneo.edu
#
# This file is part of POX.
#
# POX is free software: you can redistribute it and/or modify
# it under the terms of the GNU General Public License as published by
# the Free Software Foundation, either version 3 of the License, or
# (at your option) any later version.
#
# POX is distributed in the hope that it will be useful,
# but WITHOUT ANY WARRANTY; without even the implied warranty of
# MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE. See the
# GNU General Public License for more details.
#
# You should have received a copy of the GNU General Public License
# along with POX. If not, see <http://www.gnu.org/licenses/>.
#

"""
This is a demonstration file created to show how to obtain flow 
and port statistics from OpenFlow 1.0-enabled switches. The flow
statistics handler contains a summary of web-only traffic.
"""

# standard includes
from pox.core import core
from pox.lib.util import dpidToStr
import pox.openflow.libopenflow_01 as of
import time as t
import pickle
import pandas as pd 
import numpy as np
# include as part of the betta branch
from pox.openflow.of_json import *

log = core.getLogger()
n1=0
n2=0
table=[]
# handler for timer function that sends the requests to all the
# switches connected to the controller.
def _timer_func ():
  for connection in core.openflow._connections.values():
    global n1
    n1=t.time()
    #print 'n1a=',n1
    connection.send(of.ofp_stats_request(body=of.ofp_flow_stats_request()))
    #connection.send(of.ofp_stats_request(body=of.ofp_port_stats_request()))
  log.debug("Sent %i flow/port stats request(s)", len(core.openflow._connections))

# handler to display flow statistics received in JSON format
# structure of event.stats is defined by ofp_flow_stats()
def _handle_flowstats_received (event):
  stats = flow_stats_to_list(event.stats)
  log.debug("FlowStatsReceived from %s: %s", 
    dpidToStr(event.connection.dpid), stats)
  flowTable=[]
  if (event.connection.dpid==1):
    print "dpid=", event.connection.dpid
    # print stats
    print "************************************************"
  # #print stats
  # n2 = t.time()
  # # print 'n1b=',n1
  # # print 'n2=',n2
  # # delay=n2-n1
  # # print 'delay=',delay
  flowtable=[]
  for f in event.stats:
    # print "------------------------"  
    # actions=[]
    #print 'print',f.actions[0]['type']
    # for obj in f.actions:
       # actions.append(obj.show())  
    flowtable.append([hex(f.cookie),
                      f.duration_sec,
                      f.duration_nsec,
                      f.table_id,
                      f.priority,
                      f.packet_count,
                      f.byte_count,
                      f.idle_timeout,
                      f.hard_timeout,
                      f.match.nw_proto,
                      f.match.in_port,
                      f.match.dl_vlan,
                      f.match.dl_vlan_pcp,
                      str(f.match.dl_src),
                      str(f.match.dl_dst),
                      str(f.match.nw_src),
                      str(f.match.nw_dst),
                      str(f.match.nw_tos),
                      str(f.match.tp_src),
                      str(f.match.tp_dst)]) 
    # flow={'cookie':hex(f.cookie),
    #       'duration_sec':f.duration_sec,
    #       'duration_nsec':f.duration_nsec,
    #       'table_id':f.table_id,
    #       'priority':f.priority,
    #       'n_packets':f.packet_count,
    #       'n_bytes':f.byte_count,
    #       'idle_timeout':f.idle_timeout,
    #       'hard_timeout':f.hard_timeout,
    #       'protocol':f.match.nw_proto,
    #       'in_port':f.match.in_port,
    #       'dl_vlan':f.match.dl_vlan,
    #       'dl_vlan_pcp':f.match.dl_vlan_pcp,
    #       'dl_src':str(f.match.dl_src),
    #       'dl_dst':str(f.match.dl_dst),
    #       'nw_src':str(f.match.nw_src),
    #       'nw_dst':str(f.match.nw_dst),
    #       'nw_tos':str(f.match.nw_tos),
    #       'tp_src':str(f.match.tp_src),
    #       'tp_dst':str(f.match.tp_dst),
    #       'action':f.actions[0].show()}
    # flowTable.append(flow)
  # #print actions 
  # if len(flowTable) >0:
  #   table.append(flowTable)
  #   global cnt
  #   cnt +=1
  #   if (cnt<=135) and (cnt % 15 ==0):
  pickle.dump(table,open('./ext/outDDOS/20160202-ddos-'+str(cnt),'wb'))
  #     del table[:]
  #     print "Da in roi nhe hihi", cnt
  # print "cnt=",cnt
  # # print table
  # # print '--------------'
  # print 'time passed =',n2-start,' seconds'

  # # # print event.stats
  # # #   for obj in f.actions:
  # # #     print obj.show()
  # # # for i in range(len(flowTable)):
  # # #   print flowTable[i]
  # # print 'table_size=',len(table),len(flowtable)
  # print "number of flows=",len(event.stats)
  # print "*************************************"

# handler to display port statistics received in JSON format
def _handle_portstats_received (event):
  stats = flow_stats_to_list(event.stats)
  log.debug("PortStatsReceived from %s: %s", 
    dpidToStr(event.connection.dpid), stats)
    
# main functiont to launch the module
def launch ():
  from pox.lib.recoco import Timer
  global start
  global cnt
  cnt=0
  start=t.time()
  print 'start=',start
  # attach handsers to listners
  core.openflow.addListenerByName("FlowStatsReceived", 
    _handle_flowstats_received) 
  core.openflow.addListenerByName("PortStatsReceived", 
    _handle_portstats_received) 
  # timer set to execute every five seconds
  Timer(5, _timer_func, recurring=True)

