"""
Functions in this file can be loaded as parts of a policy stack but are not used in production any more.
"""

import time
import datetime
import re
import fnmatch

from detox.policy import Policy
from common.dataformat import Dataset, Site, DatasetReplica

class Protect(object):
    """
    Base class for callable object returning a PROTECT decision.
    """
    def __call__(self, replica):
        reason = self._do_call(replica)
        if reason is not None:
            return replica, Policy.DEC_PROTECT, reason


class Delete(object):
    """
    Base class for callable object returning a DELETE decision.
    """
    def __call__(self, replica):
        reason = self._do_call(replica)
        if reason is not None:
            return replica, Policy.DEC_DELETE, reason


class ProtectIncomplete(Protect):
    """
    PROTECT if the replica is not complete.
    Checking individual block replicas because the incompleteness of the dataset replica can be due to
    blocks not in the partition.
    """
    def _do_call(self, replica):
        if replica.is_complete:
            return

        for block_replica in replica.block_replicas:
            if not block_replica.is_complete:
                return 'Replica is not complete.'


class ProtectLocked(Protect):
    """
    PROTECT if any block of the dataset is locked.
    """
    def _do_call(self, replica):
        
        for block_replica in replica.block_replicas:
            if block_replica in replica.dataset.demand.locked_blocks:
                return 'Locked block exists.'


class ProtectCustodial(Protect):
    """
    PROTECT if the replica is custodial.
    """
    def _do_call(self, replica):
        if replica.is_custodial:
            return 'Replica is custodial.'


class ProtectDiskOnly(Protect):
    """
    PROTECT if the dataset is not on tape. 
    """
    def _do_call(self, replica):
        if replica.dataset.on_tape != Dataset.TAPE_FULL:
            return 'Dataset has no complete tape copy.'


class ProtectByNameDiskOnly(Protect):
    """
    PROTECT if the dataset matches a pattern and is not on tape.
    """
    def __init__(self, pattern, protect_match = True):
        self.pattern = re.compile(fnmatch.translate(pattern))
        self.protect_match = protect_match

    def _do_call(self, replica):
        if replica.dataset.on_tape == Dataset.TAPE_FULL:
            return

        if self.pattern.match(replica.dataset.name):
            if self.protect_match:
                return 'Dataset has no complete tape copy.'
        else:
            if not self.protect_match:
                return 'Dataset has no complete tape copy.'


class ProtectNonreadySite(Protect):
    """
    PROTECT if the site is not ready.
    """
    def _do_call(self, replica):
        if replica.site.status != Site.STAT_READY or replica.site.active == Site.ACT_IGNORE:
            return 'Site is not in ready state.'


class ProtectMinimumCopies(Protect):
    """
    PROTECT if the replica has fewer than or equal to minimum number of full copies.
    """
    def __init__(self):
        self.exceptions = [
            (re.compile(fnmatch.translate('/*/*/MINIAOD*')), 2)
        ]

    def _do_call(self, replica):
        num_copies = 0
        for replica in replica.dataset.replicas:
            if replica.is_full():
                num_copies += 1

        required_copies = 1
        for pattern, nmin in self.exceptions:
            if pattern.match(replica.dataset.name):
                required_copies = nmin
                break

        if num_copies <= required_copies:
            return 'Dataset has <= ' + str(required_copies) + ' copies.'


class ProtectNotOwnedBy(Protect):
    """
    PROTECT if the replica is not fully owned by a group.
    """
    
    def __init__(self, group_name):
        self.group_name = group_name

    def _do_call(self, replica):
        if replica.group is None or replica.group.name != self.group_name:
            return 'Not all parts of replica is owned by ' + self.group_name


class ProtectNewDiskOnly(Protect):
    """
    PROTECT if the replica is not on tape and new.
    """

    def __init__(self, threshold):
        self.threshold = threshold
        self.threshold_str = time.strftime('%Y-%m-%d', time.gmtime(self.threshold))

    def _do_call(self, replica):
        if replica.dataset.on_tape != Dataset.TAPE_FULL and replica.last_block_created > self.threshold:
            return 'Replica has no full tape copy and has a block newer than %s.' % self.threshold_str


class ProtectIncompleteTapeCopy(Protect):
    """
    PROTECT if the replica is subscribed to tape but the copy is incomplete.
    """
    def _do_call(self, replica):
        if replica.dataset.on_tape == Dataset.TAPE_PARTIAL:
            return 'A tape copy is requested but is not completed.'


class DeletePartial(Delete):
    """
    DELETE if the replica is partial.
    """
    def _do_call(self, replica):
        if replica.is_partial():
            return 'Replica is partial.'


class DeleteDeprecated(Delete):
    """
    DELETE if the dataset of the replica is deprecated.
    """
    def _do_call(self, replica):
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

    def _do_call(self, replica):
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

    def _do_call(self, replica):
        last_update = datetime.datetime.utcfromtimestamp(replica.last_block_created).date()
        if last_update > self.cutoff:
            # the dataset was updated after the cutoff -> don't delete
            return None

        # no accesses recorded ever -> delete
        if len(replica.accesses[DatasetReplica.ACC_LOCAL]) + len(replica.accesses[DatasetReplica.ACC_REMOTE]) == 0:
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

    def _do_call(self, replica):
        if replica.dataset.demand.global_usage_rank > self.threshold:
            return 'Global usage rank is above %f.' % self.threshold


class DeleteOldUnused(Delete):
    """
    DELETE if the dataset (not the replica) is old and the replica had no accesses.
    """

    def __init__(self, dataset_cutoff, replica_cutoff):
        self.dataset_threshold = time.time() - dataset_cutoff * 3600 * 24
        self.replica_threshold = time.time() - replica_cutoff * 3600 * 24

    def _do_call(self, replica):
        if replica.dataset.last_update > self.dataset_threshold:
            # this dataset is not old
            return

        if replica.last_block_created > self.replica_threshold:
            # this replica was copied too recently - someone may be still waiting to use it
            return

        # no accesses recorded ever -> delete
        if len(replica.accesses[DatasetReplica.ACC_LOCAL]) + len(replica.accesses[DatasetReplica.ACC_REMOTE]) == 0:
            return 'Dataset is old and replica is never accessed.'


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

    def __call__(self, replica):
        """
        Pass the replica through the patterns and take action on the *first* match.
        """

        for iline, (action, site_re, dataset_re) in enumerate(self.res):
            if site_re.match(replica.site.name) and dataset_re.match(replica.dataset.name):
                return replica, action, 'Pattern match: (action, site, dataset) = (%s, %s, %s)' % self.patterns[iline]
