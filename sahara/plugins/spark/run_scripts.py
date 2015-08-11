# Copyright (c) 2014 Hoang Do, Phuc Vo, P. Michiardi, D. Venzano
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#    http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or
# implied.
# See the License for the specific language governing permissions and
# limitations under the License.

import os

from sahara.openstack.common import log as logging


LOG = logging.getLogger(__name__)


def start_processes(remote, *processes):
    for proc in processes:
        if proc == "namenode":
            #remote.execute_command("sudo service hadoop-hdfs-namenode start")
            remote.execute_command("sudo -u centos JAVA_HOME=/usr/lib/jvm/java-1.7.0-openjdk.x86_64 /opt/start-jobtracker.sh")
            #remote.execute_command("sudo -u centos JAVA_HOME=/usr/lib/jvm/java-1.7.0-openjdk.x86_64 /opt/start-tasktrackers.sh")
            remote.execute_command("sudo -u centos JAVA_HOME=/usr/lib/jvm/java-1.7.0-openjdk.x86_64 /opt/start-namenode.sh")
            #remote.execute_command("sudo -u centos JAVA_HOME=/usr/lib/jvm/java-1.7.0-openjdk.x86_64 /opt/start-tasktrackers.sh")
        elif proc == "datanode":
            #remote.execute_command("sudo service hadoop-hdfs-datanode start")
            remote.execute_command("sudo -u centos JAVA_HOME=/usr/lib/jvm/java-1.7.0-openjdk.x86_64 /opt/start-datanode.sh")
        else:
            remote.execute_command("screen -d -m sudo /opt/hadoop/bin/hadoop %s" % proc)

def refresh_nodes(remote, service):
    #remote.execute_command("sudo -u hdfs hadoop %s -refreshNodes"
    #                       % service)
    remote.execute_command("sudo -u centos JAVA_HOME=/usr/lib/jvm/java-1.7.0-openjdk.x86_64 /opt/hadoop/bin/hadoop %s -refreshNodes"
                           % service)



def format_namenode(nn_remote):
    #nn_remote.execute_command("sudo -u hdfs hadoop namenode -format")
    nn_remote.execute_command("sudo -u centos JAVA_HOME=/usr/lib/jvm/java-1.7.0-openjdk.x86_64 /opt/hadoop/bin/hadoop namenode -format -force -nonInteractive")


def clean_port_hadoop(nn_remote):
    nn_remote.execute_command(("sudo netstat -tlnp"
                               "| awk '/:8020 */"
                               "{split($NF,a,\"/\"); print a[1]}'"
                               "| xargs sudo kill -9"))


def start_spark_master(nn_remote, sp_home):
    nn_remote.execute_command("bash " + os.path.join(sp_home,
						     "sbin/start-all.sh"))
    nn_remote.execute_command("bash " + os.path.join(sp_home,
                                                     "sbin/start-history-server.sh"))
def start_tasktracker(nn_remote):
    nn_remote.execute_command("bash /opt/start-tasktrackers.sh")

def start_oozie(nn_remote, namenode_hostname):
    #nn_remote.execute_command("bash cp /opt/hadoop/conf/core-site.xml /opt/oozie/conf/hadoop-conf/core-site.xml")
    nn_remote.execute_command("bash /opt/oozie/bin/oozie-setup.sh prepare-war")
    nn_remote.execute_command("bash /opt/prepare-oozie.sh")
    nn_remote.execute_command("bash /opt/oozie/bin/oozie-setup.sh sharelib create -fs hdfs://"+namenode_hostname+":8020 -locallib /opt/oozie/oozie-sharelib-4.2.0.tar.gz")
    nn_remote.execute_command("bash /opt/oozie/bin/ooziedb.sh create -sqlfile /opt/oozie/oozie.sql -run")
    nn_remote.execute_command("bash /opt/oozie/bin/oozied.sh start")

def start_dns_incrond_nginx(nn_remote):
    nn_remote.execute_command("sudo /etc/init.d/dnsmasq start")
    nn_remote.execute_command("sudo /etc/init.d/nginx start")
    nn_remote.execute_command("sudo /etc/init.d/incrond start")

def stop_spark(nn_remote, sp_home):
    nn_remote.execute_command("bash " + os.path.join(sp_home,
                                                     "sbin/stop-all.sh"))
