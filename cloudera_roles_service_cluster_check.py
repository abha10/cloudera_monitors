#!/usr/bin/python
import ConfigParser, json, time, sys
from cm_api.api_client import ApiResource
from cm_api.endpoints.services import ApiService

# Prep for reading config props from external file
CONFIG = ConfigParser.ConfigParser()
CONFIG.read("/etc/onlinesales/config/configCloudera.ini")

# This is the host that the Cloudera Manager server is running on  
CM_HOST = CONFIG.get("CM", "cm.host")

# CM admin account info

ADMIN_USER = CONFIG.get("CM", "admin.name")
ADMIN_PASS = CONFIG.get("CM", "admin.password")

#### Cluster Definition #####
CLUSTER_NAME = CONFIG.get("CM", "cluster.name")
CDH_VERSION = "CDH5"

unhealthy_roles = []

def restart_role(service, API):
    try:
        for host in API.get_all_hosts(view='full'):
            arr_len = len(host.roleRefs)
            count = 0
            i = 0
            while i < arr_len:
                count = 0
            
                while host.roleRefs[i].serviceName == service.name and service.get_role(host.roleRefs[i].roleName).healthSummary != "GOOD" and count < 2:
                    print "RESTARTING "+ service.name+"  ROLE - "+host.roleRefs[i].roleName + " on "+ host.hostname
                    cmd = service.restart_roles(host.roleRefs[i].roleName)
                    time.sleep(180)
                    count +=1
                    print "ROLE STATE: " + host.roleRefs[i].roleName + " - " + service.get_role(host.roleRefs[i].roleName).healthSummary

                #### Check whether role has started successfully or not. 
                if host.roleRefs[i].serviceName == service.name and (count >= 2 or service.get_role(host.roleRefs[i].roleName).healthSummary != "GOOD"):
                    unhealthy_roles.append(host.roleRefs[i].roleName)
                i += 1
    except Exception as api_ex:
        print api_ex
        exit(2)

def restart_service(service, api):
    try:
        count = 0;
        while (service.healthSummary != "GOOD" and count < 2):
            cmd  = service.restart().wait()
            count +=1

        #### Check if service has started successfully or not
        if (count >=2 and service.healthSummary != "GOOD" ):
            print "Unable to restart Service: "+ service.name
            restart_cluster(api)
        else:
            print service.name +" service state: " + service.healthSummary
    except Exception as ex:
        print ex
        exit(2)


def restart_cluster(API):
    try:
        # Get Cluster Health Status
        CLUSTER = API.get_cluster(CLUSTER_NAME)
        count = 0
        while (CLUSTER.entityStatus != "GOOD_HEALTH" and count < 2): 
            print "About to restart cluster."
            CLUSTER.restart().wait()
            print "Done restarting cluster."
            count +=1
        if (CLUSTER.entityStatus != "GOOD_HEALTH" and count >= 2):
            print "Unable to restart cluster - " + CLUSTER.name
            exit(2)
        else:
            print CLUSTER_NAME+ " CLUSTER STATE: " + CLUSTER.entityStatus
            exit(0)
    except Exception as api_ex:
        print api_ex
        exit(2)

### Main function ###
def main():
    API = ApiResource(CM_HOST, version=16, username=ADMIN_USER, password=ADMIN_PASS)

    for c in API.get_all_clusters():
        if c.version == "CDH5":
            cdh5 = c

    for s in cdh5.get_all_services():
        restart_role(s,API)
    if (unhealthy_roles == []):
        print("ALL ROLES: OK")
    else:
        print("Following is the list of all unhealthy Roles:\n ")
        for role in unhealthy_roles:
            print("\n\t\t" + role)

    for s in cdh5.get_all_services():
        restart_service(s,API)

if __name__ == "__main__":
   main()
