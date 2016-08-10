from common.dataformat import Dataset, Site, DatasetReplica

BOOL_TYPE, NUMERIC_TYPE, TEXT_TYPE, TIME_TYPE = range(4)

def replica_incomplete(replica):
    if replica.is_complete:
        return False

    for block_replica in replica.block_replicas:
        if not block_replica.is_complete:
            return True

    return False

def replica_has_locked_block(replica):
    for block_replica in replica.block_replicas:
        if block_replica in replica.dataset.demand.locked_blocks:
            return True

    return False

expressions = {
    'dataset.name': (lambda r: r.dataset.name, TEXT_TYPE),
    'dataset.status': (lambda r: r.dataset.status, NUMERIC_TYPE, lambda v: eval('Dataset.STAT_' + v)),
    'dataset.on_tape': (lambda r: r.dataset.on_tape, NUMERIC_TYPE, lambda v: eval('Dataset.TAPE_' + v)),
    'dataset.last_update': (lambda r: r.dataset.last_update, TIME_TYPE),
    'dataset.num_full_disk_copy': (lambda r: sum(1 for rep in r.dataset.replicas if rep.is_full()), NUMERIC_TYPE),
    'dataset.usage_rank': (lambda r: r.dataset.demand.global_usage_rank, NUMERIC_TYPE),
    'site.status': (lambda r: r.site.status, NUMERIC_TYPE, lambda v: eval('Site.STAT_' + v)),
    'site.active': (lambda r: r.site.active, NUMERIC_TYPE, lambda v: eval('Site.ACT_' + v)),
    'replica.incomplete': (replica_incomplete, BOOL_TYPE),
    'replica.last_block_created': (lambda r: r.last_block_created, TIME_TYPE),
    'replica.num_access': (lambda r: len(r.accesses[DatasetReplica.ACC_LOCAL]) + len(r.accesses[DatasetReplica.ACC_REMOTE]), NUMERIC_TYPE),
    'replica.has_locked_block': (replica_has_locked_block, BOOL_TYPE)
}
