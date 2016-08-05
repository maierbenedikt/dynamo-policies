import os
from detox.policy import Policy
from detox.policies.policies import *

default = Policy.DEC_DELETE

rule_stack = [
    ActionList(os.environ['DYNAMO_BASE'] + '/policies/detoxstacks/exceptions.list'),
    ProtectNonreadySite(),
    DeleteDeprecated(),
    ProtectIncomplete(),
    ProtectByNameDiskOnly('/*/*/RECO', protect_match = False),
    DeleteByNameOlderThan(90., 'd', '/*/*LogError*/RAW-RECO', use_dataset_time = True),
    DeleteByNameOlderThan(60., 'd', '/*/*/RAW', use_dataset_time = True),
    DeleteByNameOlderThan(90., 'd', '/*/*/RECO', use_dataset_time = True),
    DeleteUnused(400.),
    DeleteOldUnused(600., 30.),
    ProtectMinimumCopies()
]
