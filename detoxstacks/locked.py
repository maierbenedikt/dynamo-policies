from detox.policy import Policy
from detox.policies.policies import *

default = Policy.DEC_DELETE

exceptions = ActionList(os.environ['DYNAMO_BASE'] + '/policies/detoxstacks/exceptions.list')
exceptions.add_action('Keep', '*', '/*/*-PromptReco-*/*')
exceptions.add_action('Keep', '*', '/*/*/RECO')
exceptions.add_action('Keep', '*', '/*/*/RAWAODSIM')
exceptions.add_action('Keep', '*', '/*/*RunIISummer16DR80*/*')

rule_stack = [
    exceptions,
    ProtectNonreadySite(),
    DeleteDeprecated(),
    ProtectIncomplete(),
    ProtectNewDiskOnly(time.time() - 3600 * 24 * 14),
    ProtectLocked()
]
