import detox.configuration as detox_config
from detox.policy import Policy
from detox.policies.policies import *

default = Policy.DEC_DELETE

exceptions = ActionList()
for line in detox_config.routine_exceptions:
    exceptions.add_action(*line)

stack = [
    exceptions,
    ProtectNonreadySite(),
    DeleteByNameOlderThan(detox_config.reco_max_age, 'd', '/*/*/RECO'),
    DeleteDeprecated(),
    ProtectIncomplete(),
    DeleteUnused(detox_config.max_nonusage),
    ProtectMinimumCopies()
]
