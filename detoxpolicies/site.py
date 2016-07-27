"""
Classes defining site requirements.
"""

import common.configuration as config
import detox.configuration as detox_config

class GroupOccupancy(object):

    def __init__(self, groups, included_sites = None):
        self.groups = groups
        self.included_sites = included_sites

    def __call__(self, site, partition, initial):
        if self.included_sites is not None and not self.included_sites.match(site.name):
            return False

        group = groups[partition]

        if site.group_quota(group) == 0:
            return False

        if initial:
            return site.storage_occupancy(group) > detox_config.threshold_occupancy
        else:
            return site.storage_occupancy(group) > config.target_site_occupancy
