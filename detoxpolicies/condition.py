import logging

import detox.policies.expressions as expressions
import detox.policies.predicates as Predicate, InvalidExpression

logger = logging.getLogger(__name__)

class Condition(object):
    def __init__(self, text):
        self.predicates = []

        pred_strs = text.split(' and ')

        for pred_str in pred_strs:
            words = pred_str.split()

            expr = words[0]
            if expr == 'not': # special case for English language
                words[0] = words[1]
                words[1] = 'not'

            try:
                vardef = self.get_vardef(expr)
            except KeyError:
                raise InvalidExpression(expr)

            if len(words) > 2:
                operator = words[1]
            else:
                operator = ''

            rhs_expr = ' '.join(words[2:])

            self.predicates.append(Predicate.get(vardef, operator, rhs_expr))

    def match(self, obj):
        for predicate in self.predicates:
            if not predicate(obj):
                return False

        return True

class ReplicaCondition(object):
    def get_vardef(self, expr):
        return expressions.replica_vardefs[expr]
        
class SiteCondition(object):
    def __init__(self, text, partition):
        self.partition = partition

        Condition.__init__(self, text)

    def get_vardef(self, expr):
        vardef = expressions.site_vardefs[expr]
        return (vardef[0](self.partition),) + vardef[1:]
