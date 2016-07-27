import detox.configuration as detox_config
from detox.policy import Policy
from detox.policies.policies import *

default = Policy.DEC_DELETE

exceptions = ActionList()
for line in detox_config.routine_exceptions:
    exceptions.add_action(*line)

    exceptions.add_action('Keep', '*', '/*/*-PromptReco-*/*')
    exceptions.add_action('Keep', '*', '/*/*/RECO')

rule_stack = [
    exceptions,
    ProtectNonreadySite(),
    DeleteDeprecated(),
    ProtectIncomplete(),
    ProtectNewDiskOnly(time.time() - 3600 * 24 * 14),
    ProtectLocked()
]
