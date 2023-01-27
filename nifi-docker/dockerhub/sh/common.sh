#!/bin/sh -e
#    Licensed to the Apache Software Foundation (ASF) under one or more
#    contributor license agreements.  See the NOTICE file distributed with
#    this work for additional information regarding copyright ownership.
#    The ASF licenses this file to You under the Apache License, Version 2.0
#    (the "License"); you may not use this file except in compliance with
#    the License.  You may obtain a copy of the License at
#
#       http://www.apache.org/licenses/LICENSE-2.0
#
#    Unless required by applicable law or agreed to in writing, software
#    distributed under the License is distributed on an "AS IS" BASIS,
#    WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
#    See the License for the specific language governing permissions and
#    limitations under the License.

# 1 - value to search for
# 2 - value to replace
# 3 - file to perform replacement inline
prop_replace () {
  target_file=${3:-${nifi_props_file}}
  echo 'replacing target file ' ${target_file}
  sed -i -e "s|^$1=.*$|$1=$2|"  ${target_file}
}

uncomment() {
  target_file=${2}
  echo "Uncommenting ${target_file}"
  sed -i -e "s|^\#$1|$1|" ${target_file}
}

# 1 - property key to add or replace
# 2 - property value to use
# 3 - file to perform replacement inline
prop_add_or_replace () {
  target_file=${3:-${nifi_props_file}}
  echo 'adding or replacing target file ' ${target_file}
  isInFile=$(cat ${target_file} | grep -c $1)
  if [ $isInFile -eq 0 ]; then
    echo "$1=$2" >> ${target_file}
  else
    prop_replace $1 $2 $3  
  fi
}

# NIFI_HOME is defined by an ENV command in the backing Dockerfile
export nifi_bootstrap_file=${NIFI_HOME}/conf/bootstrap.conf
export nifi_props_file=${NIFI_HOME}/conf/nifi.properties
export nifi_toolkit_props_file=${HOME}/.nifi-cli.nifi.properties
export hostname=$(hostname)
