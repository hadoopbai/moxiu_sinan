#!/bin/bash
HOME=.
java -Xmx1g -Xms1g -cp $HOME/lib/diditool.jar:$HOME/lib/spark-assembly-1.6.2-hadoop2.6.0-cdh5.4.4.jar \
com.didi.etc.its.tool.OrderGPSAuto ./autoConf/autoConf.xml $1 > $HOME/log/OrderGPSAuto.log