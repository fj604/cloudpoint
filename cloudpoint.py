#!/usr/bin/env python

import sys
import requests
import json
import time


class CloudPoint:
    def __init__(self, hostname, username, password):
        self.hostname = hostname
        self.username = username
        self.password = password
        self.baseurl = "https://" + hostname
        self.cookies = None
        self.headers = {}
        self.headers["Content-Type"] = "application/json"
        return

    def request(self, method, url, json=None, params=None):
        url = self.baseurl + url
        r = requests.request(method, url, json=json, params=params, verify=False, cookies=self.cookies, headers=self.headers)
        self.cookies = r.cookies
        return r.json()

    def authenticate(self):
        payload = {}
        payload["username"] = self.username
        payload["password"] = self.password
        url = "/api/rest/authenticate"
        r = self.request("POST", url, payload)
        self.token = r["token"]
        self.headers["Authorization"] = "Bearer " + self.token
        return r

    def list_assets(self):
        self.authenticate()
        url = "/flexsnap/api/v1/assets/"
        r = self.request("GET", url)
        self.assets = r
        return r

    def get_asset(self, asset_id):
        self.authenticate()
        url = "/flexsnap/api/v1/assets/{0}".format(asset_id)
        r = self.request("GET", url)
        return r

    def list_snapshots(self, asset_id):
        self.authenticate()
        url = "/flexsnap/api/v1/assets/{0}/snapshots/".format(asset_id)
        r = self.request("GET", url)
        return r

    def get_snapshot(self, asset_id, snapshot_id):
        self.authenticate()
        url = "/flexsnap/api/v1/assets/{0}/snapshots/{1}".format(asset_id, snapshot_id)
        r = self.request("GET", url)
        return r

    def list_restore_targets(self, asset_id, snapshot_id):
        self.authenticate()
        url = "/flexsnap/api/v1/assets/{0}/snapshots/{1}/targets".format(asset_id, snapshot_id)
        r = self.request("GET", url)
        return r

    def create_snapshot(self, asset_id, snapType, name, description=""):
        self.authenticate()
        url = "/flexsnap/api/v1/assets/{0}/snapshots/".format(asset_id)
        payload = {}
        payload["snapType"] = snapType
        payload["name"] = name
        payload["description"] = description
        r = self.request("POST", url, payload)
        return r

    def delete_snapshot(self, asset_id, snapshot_id):
        self.authenticate()
        url = "/flexsnap/api/v1/assets/{0}/snapshots/{1}".format(asset_id, snapshot_id)
        r = self.request("DELETE", url)
        return r

    def restore_snapshot(self, snapshot_id, dest=None):
        self.authenticate()
        url = "/flexsnap/api/v1/assets/"
        payload = {}
        payload["snapid"] = snapshot_id
        if dest is not None:
            payload["dest"] = dest
        r = self.request("POST", url, payload)
        return r

    def restore_snapshot_overwrite_original_asset(self, snapshot_id, asset_id):
        self.authenticate()
        url = "/flexsnap/api/v1/assets/{0}".format(asset_id)
        payload = {}
        payload["snapid"] = snapshot_id
        r = self.request("PUT", url, payload)
        return r

    def list_tasks(self, status=None, run_since=None, task_type=None, limit=None, start_after=None):
        self.authenticate()
        params = {}
        if status is not None:
            params["status"] = status
        if run_since is not None:
            params["run_since"] = run_since
        if task_type is not None:
            params["taskType"] = task_type
        if limit is not None:
            params["limit"] = limit
        if start_after is not None:
            params["start_after"] = start_after
        url = "/flexsnap/api/v1/tasks/"
        r = self.request("GET", url, params=params)
        return r

    def get_task(self, task_id):
        self.authenticate()
        url = "/flexsnap/api/v1/tasks/{0}".format(task_id)
        r = self.request("GET", url)
        return r

    def delete_task(self, task_id):
        self.authenticate()
        url = "/flexsnap/api/v1/tasks/{0}".format(task_id)
        r = self.request("DELETE", url)
        return r

    def delete_tasks(self, status=None, older_than=None):
        self.authenticate()
        params = {}
        if status is not None:
            params["status"] = status
        if older_than is not None:
            params["olderThan"] = older_than
        url = "/flexsnap/api/v1/tasks/"
        r = self.request("DELETE", url, params=params)
        return r

    def asset_id(self, name):
        assets = self.list_assets()
        for asset in assets:
            if "name" in asset and asset["name"] == name:
                return asset["id"]
        return None

    def wait_for_task(self, task_id, timeout=600, check_every_seconds=10):
        end_wait = time.clock() + timeout
        while True:
            task = self.get_task(task_id)
            if task["status"] != "running" or time.clock() > end_wait:
                break
            time.sleep(check_every_seconds)
        return task

def usage():
    print "Creates a CloudPoint snapshot"
    print "Usage:", sys.argv[0], "snapshot <asset-name> <snapshot-name>"

def cloudpoint_snapshot(asset_name, snapshot_name):
    hostname = "cloudpoint"
    username = "admin"
    password = "cl0udp0int"
    c = CloudPoint(hostname, username, password)
    id = c.asset_id(asset_name)
    if id is None:
        raise Exception("AssetNotFound", "Asset not found")
    r = c.create_snapshot(id, "clone", snapshot_name, "NBU-Snapshot")
    return r


if __name__ == "__main__":
    try:
        command = sys.argv[1]
        asset_name = sys.argv[2]
        snapshot_name = sys.argv[3]
        assert command == "snapshot"
    except:
        usage()
        sys.exit()
    print "Creating snapshot", snapshot_name, "for asset", asset_name
    r = cloudpoint_snapshot(asset_name, snapshot_name)
    print "Task: ", r["name"]
    print "Status:", r["status"]
