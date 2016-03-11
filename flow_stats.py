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
import copy
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

# calculate entropy
def calculate_Entropy(df):
    # print("count=",count)
    prob = df/df.sum(axis=0)
    entropy = (-prob*np.log2(prob)).sum()
    return entropy

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
  # n2 = t.time()
    flowtable=[]
    for f in event.stats:  
      flowtable.append([hex(f.cookie),
                        # f.duration_sec,
                        # f.duration_nsec,
                        # f.table_id,
                        # f.priority,
                        f.packet_count,
                        # f.byte_count,
                        # f.idle_timeout,
                        # f.hard_timeout,
                        f.match.nw_proto,
                        # f.match.in_port,
                        # f.match.dl_vlan,
                        # f.match.dl_vlan_pcp,
                        # str(f.match.dl_src),
                        # str(f.match.dl_dst),
                        str(f.match.nw_src),
                        str(f.match.nw_dst),
                        # str(f.match.nw_tos),
                        str(f.match.tp_src),
                        str(f.match.tp_dst)]) 
    global cnt,df1,df2 
    cnt += 1
    global arr1,arr2
    if (cnt<2 ):
      print('cnt=',cnt)
      arr1 = np.array(flowtable)
      df1 = pd.DataFrame(arr1, columns=['cookie',
                    # 'duration_sec',
                    # 'duration_nsec',
                    # 'table_id',
                    # 'priority',
                    'packet_count',
                    # 'byte_count',
                    # 'idle_timeout',
                    # 'hard_timeout',
                    'nw_proto',
                    # 'in_port',
                    # 'dl_vlan',
                    # 'dl_vlan_pcp',
                    # 'dl_src',
                    # 'dl_dst',
                    'nw_src',
                    'nw_dst',
                    # 'nw_tos',
                    'tp_src',
                    'tp_dst'])
      df1.packet_count = df1.packet_count.astype(np.float)
    else:
      print('cnt=',cnt)
      arr2 = np.array(flowtable)  
      df2 = pd.DataFrame(arr2, columns=['cookie',
                    # 'duration_sec',
                    # 'duration_nsec',
                    # 'table_id',
                    # 'priority',
                    'packet_count',
                    # 'byte_count',
                    # 'idle_timeout',
                    # 'hard_timeout',
                    'nw_proto',
                    # 'in_port',
                    # 'dl_vlan',
                    # 'dl_vlan_pcp',
                    # 'dl_src',
                    # 'dl_dst',
                    'nw_src',
                    'nw_dst',
                    # 'nw_tos',
                    'tp_src',
                    'tp_dst'])
      df2.packet_count = df2.packet_count.astype(np.float)
      print 'df1 \n', df1
      print '\ndf2 \n', df2
      table1 = df1.groupby(['nw_src','nw_dst']).groups
      table2 = df2.groupby(['nw_src','nw_dst']).groups
      keys1  = set(table1.keys())
      keys2  = set(table2.keys())
      # print(keys2-keys1)
      count=0
      new_flows = pd.DataFrame(columns=['cookie',
                    # 'duration_sec',
                    # 'duration_nsec',
                    # 'table_id',
                    # 'priority',
                    'packet_count',
                    # 'byte_count',
                    # 'idle_timeout',
                    # 'hard_timeout',
                    'nw_proto',
                    # 'in_port',
                    # 'dl_vlan',
                    # 'dl_vlan_pcp',
                    # 'dl_src',
                    # 'dl_dst',
                    'nw_src',
                    'nw_dst',
                    # 'nw_tos',
                    'tp_src',
                    'tp_dst'])
      for key in (keys2 - keys1):
          # print('key=',key)
          # print('table2[key]=',table2[key])
          for i in table2[key]:
              # print('loc:',df2.loc[i])
              new_flows.loc[count]=df2.loc[i].copy()
              count = count + 1
      for key in (keys1 & keys2):
          same_entries1=[]
          same_entries2=[]
          for entry1 in table1[key]:
            print  
            for entry2 in table2[key]:
                # print(df2.loc[entry2]['v1'])
                if (entry2 not in same_entries2) and (df1.loc[entry1]['tp_src']==df2.loc[entry2]['tp_src']) and ((df1.loc[entry1]['tp_dst']==df2.loc[entry2]['tp_dst'])) and (df1.loc[entry1]['nw_proto']==df2.loc[entry2]['nw_proto']):
                    same_entries1.append(entry1)
                    same_entries2.append(entry2)
                    # print("same1 :",same_entries1)
                    # print("same2 :",same_entries2)
                    break
          # print(type(table2))
          # print(table2[key])
          i=0
          for entry2 in table2[key]:
            # print('entry2=',entry2)
            if entry2 not in same_entries2:
              # if this is new flow
              new_flows.loc[count]=df2.loc[entry2].copy()
              count=count+1
              # print('i=',i)
              # print("-----")
            else:
              # print("i=",i)
              # print("len same entries1 = ",len(same_entries1))
              # print("df2 entry2 pkt count =",df2.loc[entry2]['packet_count'])
              # print("df1 same [i] pkt count= ",df1.loc[same_entries1[i]]['packet_count'])
              # print(df2.loc[entry2]['packet_count'] < df1.loc[same_entries1[i]]['packet_count'])
              # print "df2",df2.loc[entry2]
              # print "df1",df1.loc[same_entries1[i]]
              if df2.loc[entry2]['packet_count'] < df1.loc[same_entries1[i]]['packet_count']:
                  # print("df2 entry2 pkt count =",df2.loc[entry2]['packet_count'])
                  # print("df1 same [i] pkt count= ",df1.loc[same_entries1[i]]['packet_count'])
                  # means this is dead and there is a new flow entry
                  new_flows.loc[count]=df2.loc[entry2].copy()
                  count=count+1
              elif df2.loc[entry2]['packet_count'] > df1.loc[same_entries1[i]]['packet_count']:
                  # print('type df2 entry= ',type(df2.loc[entry2]))
                  pseudo_flow = df2.loc[entry2].copy()
                  # -1 to simulate that this is a new flow entry
                  # print('df2 entry2 pkt count =',df2.loc[entry2]['packet_count'])
                  # print('df1 same [i] pkt count =',df1.loc[same_entries1[i]]['packet_count'])
                  pseudo_flow['packet_count'] = int(df2.loc[entry2]['packet_count']) - int(df1.loc[same_entries1[i]]['packet_count']) -1
                  #unknown fields to None
                  # print('pseudo pkt count =',pseudo_flow['packet_count'])
                  # print('df2 entry2 pkt count =',df2.loc[entry2]['packet_count'])
                  pseudo_flow['cookie']=None
                  new_flows.loc[count]=pseudo_flow
                  count=count+1
              i=i+1
              # print("*****")
      # print(same_entries1)
      # print(same_entries2)
      print 'new_flows \n',new_flows
      # print 'ip_src', new_flows.nw_src.unique
      ent_ip_src = calculate_Entropy(new_flows.groupby(['nw_src'])['packet_count'].sum())
      ent_tp_src = calculate_Entropy(new_flows.groupby(['tp_src'])['packet_count'].sum())
      ent_tp_dst = calculate_Entropy(new_flows.groupby(['tp_dst'])['packet_count'].sum())
      ent_packet_type = calculate_Entropy(new_flows.groupby(['nw_proto'])['packet_count'].sum())
      total_packets = new_flows.packet_count.sum()
      feature_vector = pd.Series([ent_ip_src,ent_tp_src,ent_tp_dst,ent_packet_type,total_packets], index=['ent_ip_src', 'ent_tp_src', 'ent_tp_dst', 'ent_packet_type', 'total_packets'])
      print "\nFeature list \n", feature_vector
      df1 = df2.copy()
      


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

