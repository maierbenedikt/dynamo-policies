from detox.policy import Policy
from detox.policies.policies import *

default = Policy.DEC_DELETE

exceptions = ActionList(os.path.dirname(__file__) + '/exceptions.list')
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
