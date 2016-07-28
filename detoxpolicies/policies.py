import time
import datetime
import re
import fnmatch

from detox.policy import Policy
from common.dataformat import Dataset, Site

class Protect(object):
    """
    Base class for callable object returning a PROTECT decision.
    """
    def __call__(self, replica, dataset_demand):
        reason = self._do_call(replica, dataset_demand)
        if reason is not None:
            return replica, Policy.DEC_PROTECT, reason


class Delete(object):
    """
    Base class for callable object returning a DELETE decision.
    """
    def __call__(self, replica, dataset_demand):
        reason = self._do_call(replica, dataset_demand)
        if reason is not None:
            return replica, Policy.DEC_DELETE, reason


class ProtectIncomplete(Protect):
    """
    PROTECT if the replica is not complete.
    Checking individual block replicas because the incompleteness of the dataset replica can be due to
    blocks not in the partition.
    """
    def _do_call(self, replica, dataset_demand):
        if replica.is_complete:
            return

        for block_replica in replica.block_replicas:
            if not block_replica.is_complete:
                return 'Replica is not complete.'


class ProtectLocked(Protect):
    """
    PROTECT if any block of the dataset is locked.
    """
    def _do_call(self, replica, dataset_demand):
        
        for block_replica in replica.block_replicas:
            if block_replica in dataset_demand.locked_blocks:
                return 'Locked block exists.'


class ProtectCustodial(Protect):
    """
    PROTECT if the replica is custodial.
    """
    def _do_call(self, replica, dataset_demand):
        if replica.is_custodial:
            return 'Replica is custodial.'


class ProtectDiskOnly(Protect):
    """
    PROTECT if the dataset is not on tape. 
    """
    def _do_call(self, replica, dataset_demand):
        if not replica.dataset.on_tape:
            return 'Dataset has no complete tape copy.'


class ProtectByNameDiskOnly(Protect):
    """
    PROTECT if the dataset matches a pattern and is not on tape.
    """
    def __init__(self, pattern):
        self.pattern = re.compile(fnmatch.translate(pattern))

    def _do_call(self, replica, dataset_demand):
        if not self.pattern.match(replica.dataset.name):
            return

        if not replica.dataset.on_tape:
            return 'Dataset has no complete tape copy.'


class ProtectNonreadySite(Protect):
    """
    PROTECT if the site is not ready.
    """
    def _do_call(self, replica, dataset_demand):
        if replica.site.status != Site.STAT_READY or replica.site.active == Site.ACT_IGNORE:
            return 'Site is not in ready state.'


class ProtectMinimumCopies(Protect):
    """
    PROTECT if the replica has fewer than or equal to minimum number of full copies.
    """
    def _do_call(self, replica, dataset_demand):
        required_copies = dataset_demand.required_copies
        num_copies = 0
        for replica in replica.dataset.replicas:
            if replica.is_full():
                num_copies += 1

        if num_copies <= required_copies:
            return 'Dataset has <= ' + str(required_copies) + ' copies.'


class ProtectNotOwnedBy(Protect):
    """
    PROTECT if the replica is not fully owned by a group.
    """
    
    def __init__(self, group_name):
        self.group_name = group_name

    def _do_call(self, replica, dataset_demand):
        if replica.group is None or replica.group.name != self.group_name:
            return 'Not all parts of replica is owned by ' + self.group_name


class ProtectNewDiskOnly(Protect):
    """
    PROTECT if the replica is not on tape and new.
    """

    def __init__(self, threshold):
        self.threshold = threshold
        self.threshold_str = time.strftime('%Y-%m-%d', time.gmtime(self.threshold))

    def _do_call(self, replica, dataset_demand):
        if not replica.dataset.on_tape and replica.last_block_created > self.threshold:
            return 'Replica has no full tape copy and has a block newer than %s.' % self.threshold_str


class DeletePartial(Delete):
    """
    DELETE if the replica is partial.
    """
    def _do_call(self, replica, dataset_demand):
        if replica.is_partial():
            return 'Replica is partial.'


class DeleteDeprecated(Delete):
    """
    DELETE if the dataset of the replica is deprecated.
    """
    def _do_call(self, replica, dataset_demand):
        if replica.dataset.status == Dataset.STAT_DEPRECATED:
            return 'Dataset is deprecated.'


class DeleteOlderThan(Delete):
    """
    DELETE if the replica is not accessed for more than a set time.
    """

    def __init__(self, threshold, unit):
        self.threshold_text = '%.1f%s' % (threshold, unit)

        if unit == 'y':
            threshold *= 365.
        if unit == 'y' or unit == 'd':
            threshold *= 24.
        if unit == 'y' or unit == 'd' or unit == 'h':
            threshold *= 3600.

        cutoff_timestamp = time.time() - threshold
        cutoff_datetime = datetime.datetime.utcfromtimestamp(cutoff_timestamp)
        self.cutoff = cutoff_datetime.date()


class DeleteByNameOlderThan(DeleteOlderThan):
    """
    DELETE RECO replica if it was created more than threshold ago.
    """

    def __init__(self, threshold, unit, pattern, use_dataset_time = False):
        DeleteOlderThan.__init__(self, threshold, unit)
        self.pattern = re.compile(fnmatch.translate(pattern))
        self.use_dataset_time = use_dataset_time

    def _do_call(self, replica, dataset_demand):
        if not self.pattern.match(replica.dataset.name):
            return

        if self.use_dataset_time:
            last_update = datetime.datetime.utcfromtimestamp(replica.dataset.last_update).date()
        else:
            last_update = datetime.datetime.utcfromtimestamp(replica.last_block_created).date()

        if last_update < self.cutoff:
            return 'Replica was updated more than ' + self.threshold_text + ' ago.'


class DeleteNotAccessedFor(DeleteOlderThan):
    """
    DELETE if the replica is not accessed for more than a set time.
    """

    def __init__(self, threshold, unit):
        DeleteOlderThan.__init__(self, threshold, unit)

    def _do_call(self, replica, dataset_demand):
        last_update = datetime.datetime.utcfromtimestamp(replica.last_block_created).date()
        if last_update > self.cutoff:
            # the dataset was updated after the cutoff -> don't delete
            return None

        # no accesses recorded ever -> delete
        if len(replica.accesses) == 0:
            return 'Replica was created on ' + last_update.strftime('%Y-%m-%d') + ' but is never accessed.'

        for acc_type, records in replica.accesses.items(): # remote and local
            if len(records) == 0:
                continue

            last_acc_date = max(records.keys()) # datetime.date object set to UTC

            if last_acc_date > self.cutoff:
                return None
            
        return 'Last access is older than ' + self.threshold_text + '.'


class DeleteUnused(Delete):
    """
    DELETE if the dataset global usage rank (scale low to high) is above threshold.
    """

    def __init__(self, threshold):
        self.threshold = threshold

    def _do_call(self, replica, dataset_demand):
        if dataset_demand.global_usage_rank > self.threshold:
            return 'Global usage rank is above %f.' % self.threshold


class ActionList(object):
    """
    Take decision from a list of policies.
    The list should have a decision, a site, and a dataset (wildcard allowed for both) per row, separated by white spaces.
    Any line that does not match the pattern
      (Keep|Delete) <site> <dataset>
    is ignored.
    """

    def __init__(self, list_path = ''):
        self.res = [] # (action, site_re, dataset_re)
        self.patterns = [] # (action_str, site_pattern, dataset_pattern)

        if list_path:
            self.load_list(list_path)

    def add_action(self, action_str, site_pattern, dataset_pattern):
        site_re = re.compile(fnmatch.translate(site_pattern))
        dataset_re = re.compile(fnmatch.translate(dataset_pattern))

        if action_str == 'Keep':
            action = Policy.DEC_PROTECT
        else:
            action = Policy.DEC_DELETE

        self.res.append((action, site_re, dataset_re))
        self.patterns.append((action_str, site_pattern, dataset_pattern))

    def load_list(self, list_path):
        with open(list_path) as deletion_list:
            for line in deletion_list:
                matches = re.match('\s*(Keep|Delete)\s+([A-Za-z0-9_*]+)\s+(/[\w*-]+/[\w*-]+/[\w*-]+)', line.strip())
                if not matches:
                    continue

                action_str = matches.group(1)
                site_pattern = matches.group(2)
                dataset_pattern = matches.group(3)

                self.add_action(action_str, site_pattern, dataset_pattern)

    def load_lists(self, list_paths):
        for list_path in list_paths:
            self.load_list(list_path)

    def __call__(self, replica, dataset_demand):
        """
        Pass the replica through the patterns and take action on the *first* match.
        """

        for iline, (action, site_re, dataset_re) in enumerate(self.res):
            if site_re.match(replica.site.name) and dataset_re.match(replica.dataset.name):
                return replica, action, 'Pattern match: (action, site, dataset) = (%s, %s, %s)' % self.patterns[iline]
