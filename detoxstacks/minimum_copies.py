import os
from detox.policy import Policy
from detox.policies.policies import *

default = Policy.DEC_DELETE

rule_stack = [
    ActionList(os.environ['DYNAMO_BASE'] + '/policies/detoxstacks/exceptions.list'),
    ProtectNonreadySite(),
    DeleteByNameOlderThan(90., 'd', '/*/*/RECO'),
    DeleteDeprecated(),
    ProtectIncomplete(),
    DeleteUnused(400.),
    ProtectMinimumCopies()
]
