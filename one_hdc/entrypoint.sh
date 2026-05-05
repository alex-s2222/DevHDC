#!/bin/bash
set -e

echo "Starting Hadoop container as SERVICE=$SERVICE"

# =========================
# CONFIGURE DEFAULT FS
# =========================
export CORE_CONF_fs_defaultFS=${CORE_CONF_fs_defaultFS:-hdfs://namenode:8020}

# =========================
# WAIT FUNCTION
# =========================
wait_for() {
  host=$1
  port=$2

  echo "Waiting for $host:$port ..."
  until nc -z $host $port; do
    sleep 2
  done
  echo "$host:$port is available"
}

# =========================
# HDFS FORMAT (only once)
# =========================
format_namenode() {
  if [ ! -d "/hadoop/dfs/name/current" ]; then
    echo "Formatting NameNode..."
    hdfs namenode -format -force -nonInteractive
  fi
}

# =========================
# SERVICE ROUTER
# =========================

case "$SERVICE" in

  namenode)
    format_namenode
    echo "Starting NameNode..."
    exec hdfs namenode
    ;;

  datanode)
    wait_for namenode 8020
    echo "Starting DataNode..."
    exec hdfs datanode
    ;;

  resourcemanager)
    wait_for namenode 8020
    echo "Starting ResourceManager..."
    exec yarn resourcemanager
    ;;

  nodemanager)
    wait_for resourcemanager 8032
    echo "Starting NodeManager..."
    exec yarn nodemanager
    ;;

  *)
    echo "Unknown SERVICE=$SERVICE"
    echo "Use: namenode | datanode | resourcemanager | nodemanager"
    exec bash
    ;;
esac