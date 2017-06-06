#!/usr/bin/env python
# -*- coding: utf-8 -*-

"""
Get All vm objects placement in vSAN cluster envirnment.
"""


from pyVim.connect import SmartConnect, Disconnect
from pyVmomi import vim

import sys
import ssl
import atexit
import argparse
import getpass
import json
# import VSAN API bidings
import vsanmgmtObjects
import vsanapiutils
from operator import itemgetter, attrgetter

def GetArgs():
    parser = argparse.ArgumentParser(
        description='Process args for VSAN SDK application'
    )
    parser.add_argument('-s', '--host', required=True, action='store', help='Remote host to connect to')
    parser.add_argument('-o', '--port', type=int, default=443, action='store', help='Port to connect on')
    parser.add_argument('-u', '--user', required=True, action='store', help='User name to use when connecting to host')
    parser.add_argument('-p', '--password', required=False, action='store', help='Password to use when connecting to host')
    parser.add_argument('--cluster', dest='clusterName', metavar="CLUSTER", default='CL')
    parser.add_argument('--allflash', action='store_true')

    args = parser.parse_args()
    return args


def getClusterInstance(clusterName, serviceInstance):
    content = serviceInstance.RetrieveContent()
    searchIndex = content.searchIndex
    datacenters = content.rootFolder.childEntity
    for datacenter in datacenters:
        cluster = searchIndex.FindChild(datacenter.hostFolder, clusterName)
        if cluster is not None:
            return cluster
    return None

def CollectMultiple(content, objects, parameters, handleNotFound=True):
    if len(objects) == 0:
        return {}
    result = None
    pc = content.propertyCollector
    propSet = [vim.PropertySpec(
        type=objects[0].__class__,
        pathSet=parameters
    )]

    while result == None and len(objects) > 0:
        try:
            objectSet = []
            for obj in objects:
                objectSet.append(vim.ObjectSpec(obj=obj))
            specSet = [vim.PropertyFilterSpec(objectSet=objectSet, propSet=propSet)]
            result = pc.RetrieveProperties(specSet=specSet)
        except vim.ManagedObjectNotFound as ex:
            objects.remove(ex.obj)
            result = None

    out = {}
    for x in result:
        out[x.obj] ={}
        for y in x.propSet:
            out[x.obj][y.name] = y.val
    return out

# Start program
def main():
    args = GetArgs()
    if args.password:
        password = args.password
    else:
        password = getpass.getpass(prompt='Enter password for host %s and '
                                  'user %s: ' % (args.host, args.user))

    # turn off the hostname checking and client side verification for SSL
    context = None
    if sys.version_info[:3] > (2, 7, 8):
        context = ssl.create_default_context()
        context.check_hostname = False
        context.verify_mode = ssl.CERT_NONE

    si = SmartConnect(host=args.host,
                      user=args.user,
                      pwd=password,
                      port=int(args.port),
                      sslContext=context)

    atexit.register(Disconnect, si)

    cluster = getClusterInstance(args.clusterName, si)

    objects = {}
    for ds in cluster.datastore:
        if ds.summary.type == 'vsan':
            vsands = ds
            vms = vsands.vm
            vmsProps = CollectMultiple(si.content, vms,
                                         ['name', 'config.hardware.device', 'summary.config'])
            for vm in vmsProps.keys():
                # Get vmdk object information
                for device in vmsProps[vm]['config.hardware.device']:
                    if hasattr(device.backing, 'fileName'):
                        # Dict "vmdk-object ID":  "vm-object name", "vm-name"
                        objects[device.backing.backingObjectId] = [vm, device.backing.fileName.split("/")[1].split(".")[0]]
                # Get namespace object information
                nameSpaceUuid = vmsProps[vm]['summary.config'].vmPathName.split("] ")[1].split("/")[0]
                nameSpaceName = vmsProps[vm]['summary.config'].name
                # Dict "vmhome-object ID": "vm-object name", "vmhone-object ID"
                objects[nameSpaceUuid] = [vm, nameSpaceUuid, nameSpaceName]

    hostProps = CollectMultiple(si.content, cluster.host,
                                ['name', 'configManager.vsanSystem', 'configManager.vsanInternalSystem', 'configManager.storageSystem', 'configManager.datastoreSystem'])
    hosts = hostProps.keys()

    for host in hosts:
        print '\n**** ' + hostProps[host]['name'] + ' ****\n'
        disksAll = hostProps[host]['configManager.vsanSystem'].QueryDisksForVsan()
        disks = {}
        for result in disksAll:
            if result.state == 'inUse':
                disks[result.vsanUuid] = result.disk.canonicalName
        for uuid, name in disks.iteritems():
            print '\t- disk canonical name: %s (uuid: %s)' % (name, uuid)
            vsanIntSys = hostProps[host]['configManager.vsanInternalSystem']
            queryObjects = vsanIntSys.QueryObjectsOnPhysicalVsanDisk(uuid)
            output = json.loads(queryObjects)
            objectsOnDisk = output['objects_on_disks'][uuid]
            if len(objectsOnDisk) == 0:
                print '\t\tNo Objects on this disk'
            else:
                for obj in objectsOnDisk:
                    if obj in objects:
                        if obj == objects[obj][1]:
                            print '\t\t- VM HOME: %s' %(objects[obj][2])
                        else:
                            print '\t\t- VMDK: %s' %(objects[obj][1])
                    else:
                        print '\t\t- Cannot attribute object "%s" to any VM, may be swap?' %(obj)


# Start program
if __name__ == "__main__":
    main()
