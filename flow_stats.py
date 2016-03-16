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
  global cnt,df1,df2 
  global arr1,arr2
  if (event.connection.dpid==1):
    print "dpid=", event.connection.dpid
    # print stats
    print "************************************************"
    n1=t.time()                              ###########################n1#####################
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
    lenFlow = len(flowtable)
    n2=t.time()                              ###########################n2-n1 = getflow#####################
    print("n2-n1=",n2-n1)
    if (lenFlow!=0) and (cnt<1):
      print('cnt=',cnt)
      cnt = cnt + 1
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
    elif (lenFlow!=0) and (cnt >=1):
      n3=t.time()           ######################################################### n3 ########
      print('cnt=',cnt)
      cnt = cnt+1
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
      # new_flows = pd.DataFrame(columns=['cookie',
      #               # 'duration_sec',
      #               # 'duration_nsec',
      #               # 'table_id',
      #               # 'priority',
      #               'packet_count',
      #               # 'byte_count',
      #               # 'idle_timeout',
      #               # 'hard_timeout',
      #               'nw_proto',
      #               # 'in_port',
      #               # 'dl_vlan',
      #               # 'dl_vlan_pcp',
      #               # 'dl_src',
      #               # 'dl_dst',
      #               'nw_src',
      #               'nw_dst',
      #               # 'nw_tos',
      #               'tp_src',
      #               'tp_dst'])
      new_flows = pd.DataFrame(columns=['nw_src',
                    # 'duration_sec',
                    # 'duration_nsec',
                    # 'table_id',
                    # 'priority',
                    'nw_dst',
                    # 'byte_count',
                    # 'idle_timeout',
                    # 'hard_timeout',
                    'cookie',
                    # 'in_port',
                    # 'dl_vlan',
                    # 'dl_vlan_pcp',
                    # 'dl_src',
                    # 'dl_dst',
                    'packet_count',
                    'nw_proto',
                    # 'nw_tos',
                    'tp_src',
                    'tp_dst'])
      df2.packet_count = df2.packet_count.astype(np.float)
      print 'df1 \n', df1
      print 'df2 \n', df2
      ############               add diff IP_src,IP_dst
###########################################  v.1 ####################
      # merg=pd.merge(df1, df2, on=['nw_src', 'nw_dst'], how='inner')
      # print "MERG=",merg
      # merg1=merg.drop('tp_src_x',axis=1)
      # merg1=merg1.drop('tp_dst_x',axis=1)
      # merg1=merg1.drop('cookie_x',axis=1)
      # merg1=merg1.drop('packet_count_x',axis=1)
      # merg1=merg1.drop('nw_proto_x',axis=1)
      # merg1.columns=['nw_src','nw_dst','cookie','packet_count','nw_proto','tp_src','tp_dst']
      # print "MERG 1=",merg1
      # ds1 = set(tuple(line) for line in merg1.values)
      # ds2 = set(tuple(line) for line in df2.values)
      # print "DS1=",ds1
      # print "DS2 -DS1=\n",list(ds2.difference(ds1))
      # df = pd.DataFrame(list(ds2.difference(ds1)),columns=df2.columns)
      # print "DF=\n",df
###########################################  v.1 ####################

################# v.2 #######################
      df1.set_index(['nw_src', 'nw_dst'], inplace=True)
      df2.set_index(['nw_src', 'nw_dst'], inplace=True)
      s= df2.join(df1,lsuffix='_x',rsuffix='_y')
      print "S=\n",s
      s=s.loc[np.isnan(s.packet_count_y)]
      s.reset_index(level=['nw_src', 'nw_dst'], inplace=True)
      s=s.drop('tp_src_y',axis=1)
      s=s.drop('tp_dst_y',axis=1)
      s=s.drop('cookie_y',axis=1)
      s=s.drop('packet_count_y',axis=1)
      s=s.drop('nw_proto_y',axis=1)
      s.columns=['nw_src','nw_dst','cookie','packet_count','nw_proto','tp_src','tp_dst']
      new_flows=new_flows.append(s)
      print "NEW=\n",new_flows
      df1.reset_index(level=['nw_src', 'nw_dst'], inplace=True)
      df2.reset_index(level=['nw_src', 'nw_dst'], inplace=True)
################# v.2 #######################

      ################################## Same IP, port  & protocol differerent ***********************************
      merg=pd.merge(df1, df2, on=['nw_src', 'nw_dst'], how='inner')
      print "MERG=\n",merg
      df=merg.loc[(merg['tp_src_x']!=merg['tp_src_y']) |  (merg['tp_dst_x']!=merg['tp_dst_y']) | (merg['nw_proto_x']!=merg['nw_proto_y'])]
      # print "merg=\n", merg
      # print "DF=\n", df
      df=df.drop('tp_src_x',axis=1)
      df=df.drop('tp_dst_x',axis=1)
      df=df.drop('cookie_x',axis=1)
      df=df.drop('packet_count_x',axis=1)
      df=df.drop('nw_proto_x',axis=1)
      df.columns=['nw_src','nw_dst','cookie','packet_count','nw_proto','tp_src','tp_dst'] 
      new_flows=new_flows.append(df)
      print "NEW=\n",new_flows
      # ##########################################          New pkt count < old pkt count => new flow => add
      # print "MERG=",merg
      df=merg.loc[(merg['tp_src_x']==merg['tp_src_y']) &  (merg['tp_dst_x']==merg['tp_dst_y']) & (merg['nw_proto_x']==merg['nw_proto_y']) & (merg['packet_count_y']<merg['packet_count_x'])]
      # print "DF=",df
      df=df.drop('tp_src_x',axis=1)
      df=df.drop('tp_dst_x',axis=1)
      df=df.drop('cookie_x',axis=1)
      df=df.drop('packet_count_x',axis=1)
      df=df.drop('nw_proto_x',axis=1)
      df.columns=['nw_src','nw_dst','cookie','packet_count','nw_proto','tp_src','tp_dst'] 
      new_flows=new_flows.append(df)
      print "NEW=\n",new_flows
      # ###########################################          new pkt count > old => more pkt of same flow => add
      # print merg
      df=merg.loc[(merg['tp_src_x']==merg['tp_src_y']) &  (merg['tp_dst_x']==merg['tp_dst_y']) & (merg['nw_proto_x']==merg['nw_proto_y']) & (merg['packet_count_y']>merg['packet_count_x'])]
      df.packet_count_y=df.packet_count_y-df.packet_count_x-1
      df=df.drop('tp_src_x',axis=1)
      df=df.drop('tp_dst_x',axis=1)
      df=df.drop('cookie_x',axis=1)
      df=df.drop('packet_count_x',axis=1)
      df=df.drop('nw_proto_x',axis=1)
      df.columns=['nw_src','nw_dst','cookie','packet_count','nw_proto','tp_src','tp_dst'] 
      # print "DF=\n",df
      # test=pd.DataFrame(np.random.randn(5,7),columns=['nw_src','nw_dst','cookie','packet_count','nw_proto','tp_src','tp_dst'])
      new_flows=new_flows.append(df)
      print "NEW=\n",new_flows

      # print 'df1 \n', df1
      # print '\ndf2 \n', df2
      # table1 = df1.groupby(['nw_src','nw_dst']).groups            # return dictionary
      # table2 = df2.groupby(['nw_src','nw_dst']).groups            # return dictionary
      # table1 = pd.Series(table1)
      # table2 = pd.Series(table2)
      # keys1  = set(table1.keys())
      # keys2  = set(table2.keys())
      # diff = np.asarray(keys2-keys1)
      # comm = np.asarray(keys2 & keys1)
      # print "comm=",comm,"type=",type(comm)
      # # print "diff=",diff
      # # keys1 = np.array(keys1)
      # # keys2 = np.array(keys2)
      # # print(keys2-keys1)
      # print("type key1 =",type(keys1),"type table1 =",type(table1))
      # print("type key2 =",type(keys2),"type table2 =",type(table2))
      # # print(keys2-keys1)
      # n4 = t.time()                              ##################   n4 - n3 = grouping time####################
      # print("n4-n3=",n4-n3)
      # count=0
      # new_flows = pd.DataFrame(columns=['cookie',
      #               # 'duration_sec',
      #               # 'duration_nsec',
      #               # 'table_id',
      #               # 'priority',
      #               'packet_count',
      #               # 'byte_count',
      #               # 'idle_timeout',
      #               # 'hard_timeout',
      #               'nw_proto',
      #               # 'in_port',
      #               # 'dl_vlan',
      #               # 'dl_vlan_pcp',
      #               # 'dl_src',
      #               # 'dl_dst',
      #               'nw_src',
      #               'nw_dst',
      #               # 'nw_tos',
      #               'tp_src',
      #               'tp_dst'])
      # n5 = t.time()             ############################ n5  #########################
      # for key in (keys2 - keys1):
      #     # print('key=',key)
      #     # print('table2[key]=',table2[key])
      #     for i in table2[key]:
      #         # print('loc:',df2.loc[i])
      #         new_flows.loc[count]=df2.loc[i].copy()
      #         count = count + 1
      # n6 = t.time()          ###################### n6 -n5 = get diff IP src, dst flow  #################
      # print("n6-n5=",n6-n5)
      # print("type nw_src,nw_dst = ",type(df1.nw_src[2]),type(df2.nw_dst[2]))
      # for key in (keys1 & keys2):
      #     same_entries1=[]
      #     same_entries2=[]

      #     n7 = t.time()    #####################  n7  ###########################  

      #     for entry1 in table1[key]:
      #       for entry2 in table2[key]:
      #           # print(df2.loc[entry2]['v1'])
      #           if (entry2 not in same_entries2) and (df1.loc[entry1]['tp_src']==df2.loc[entry2]['tp_src']) and ((df1.loc[entry1]['tp_dst']==df2.loc[entry2]['tp_dst'])) and (df1.loc[entry1]['nw_proto']==df2.loc[entry2]['nw_proto']):
      #               same_entries1.append(entry1)
      #               same_entries2.append(entry2)
      #               # print("same1 :",same_entries1)
      #               # print("same2 :",same_entries2)
      #               break
      #     # print(type(table2))
      #     # print(table2[key])
      #     n8 = t.time()          ############################   n8 - n7 = create same entries #########################
      #     print "n8-n7=",n8-n7
      #     i=0
      #     for entry2 in table2[key]:
      #       # print('entry2=',entry2)
      #       if entry2 not in same_entries2:
      #         # if this is new flow
      #         new_flows.loc[count]=df2.loc[entry2].copy()
      #         count=count+1
      #         # print('i=',i)
      #         # print("-----")
      #       else:
      #         # print("i=",i)
      #         # print("len same entries1 = ",len(same_entries1))
      #         # print("df2 entry2 pkt count =",df2.loc[entry2]['packet_count'])
      #         # print("df1 same [i] pkt count= ",df1.loc[same_entries1[i]]['packet_count'])
      #         # print(df2.loc[entry2]['packet_count'] < df1.loc[same_entries1[i]]['packet_count'])
      #         # print "df2",df2.loc[entry2]
      #         # print "df1",df1.loc[same_entries1[i]]
      #         if df2.loc[entry2]['packet_count'] < df1.loc[same_entries1[i]]['packet_count']:
      #             # print("df2 entry2 pkt count =",df2.loc[entry2]['packet_count'])
      #             # print("df1 same [i] pkt count= ",df1.loc[same_entries1[i]]['packet_count'])
      #             # means this is dead and there is a new flow entry
      #             new_flows.loc[count]=df2.loc[entry2].copy()
      #             count=count+1
      #         elif df2.loc[entry2]['packet_count'] > df1.loc[same_entries1[i]]['packet_count']:
      #             # print('type df2 entry= ',type(df2.loc[entry2]))
      #             pseudo_flow = df2.loc[entry2].copy()
      #             # -1 to simulate that this is a new flow entry
      #             # print('df2 entry2 pkt count =',df2.loc[entry2]['packet_count'])
      #             # print('df1 same [i] pkt count =',df1.loc[same_entries1[i]]['packet_count'])
      #             pseudo_flow['packet_count'] = int(df2.loc[entry2]['packet_count']) - int(df1.loc[same_entries1[i]]['packet_count']) -1
      #             #unknown fields to None
      #             # print('pseudo pkt count =',pseudo_flow['packet_count'])
      #             # print('df2 entry2 pkt count =',df2.loc[entry2]['packet_count'])
      #             pseudo_flow['cookie']=None
      #             new_flows.loc[count]=pseudo_flow
      #             count=count+1
      #         i=i+1
      #     n9 = t.time()  ####################  n9-n8 = add diff flow time    ##################    
      #     print "n9-n8=",n9-n8
          ###############  n8 - n7   ###################################
              # print("*****")
      # print(same_entries1)
      # print(same_entries2)
      # print 'new_flows \n',new_flows
      # print 'ip_src', new_flows.nw_src.unique
      # n10 = t.time()    ########################  n10 - n6 = 2.1s UDP 10packet/s  ###############################
      # print(" time to get new flow n10 -n6=",n10-n6)
      ent_ip_src = calculate_Entropy(new_flows.groupby(['nw_src'])['packet_count'].sum())
      ent_tp_src = calculate_Entropy(new_flows.groupby(['tp_src'])['packet_count'].sum())
      ent_tp_dst = calculate_Entropy(new_flows.groupby(['tp_dst'])['packet_count'].sum())
      ent_packet_type = calculate_Entropy(new_flows.groupby(['nw_proto'])['packet_count'].sum())
      total_packets = new_flows.packet_count.sum()
      feature_vector = pd.Series([ent_ip_src,ent_tp_src,ent_tp_dst,ent_packet_type,total_packets], index=['ent_ip_src', 'ent_tp_src', 'ent_tp_dst', 'ent_packet_type', 'total_packets'])
      print "Feature list \n ", feature_vector
      # n11 = t.time()      ##################### n11-n10 = time to cal FEature  #############################
      # print("time to get FVector n11-n10=", n11-n10)
      df1 = df2.copy()
      n12 = t.time()     ################  n12-n1 = total time ##############
      print('total delay =', n12 -n1)
      


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

