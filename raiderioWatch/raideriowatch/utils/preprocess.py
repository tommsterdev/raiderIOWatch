from functools import partial, reduce
from typing import Callable
from decimal import Decimal
from models.member import Member

Preprocessor = Callable[[list[Member]], list[Member]]


def filter_rank(members: list[Member], min_rank: int) -> list[Member]:
    return [member for member in members if member.rank < min_rank]


def filter_ilvl(members: list[Member], min_ilvl: Decimal) -> list[Member]:
    return [member for member in members if member.ilvl and member.ilvl >= min_ilvl]


def filter_score(members: list[Member], min_score: Decimal) -> list[Member]:
    return [member for member in members if member.score and member.ilvl >= min_score]


def compose(*functions: Preprocessor) -> Preprocessor:
    return reduce(lambda f,g : lambda x: g(f(x)), functions)

def preprocess(members: list[Member]) -> list[Member]:
    filter_low_rank = filter_rank(members=members, min_rank=4)
    return filter_low_rank

def postprocess(members: list[Member]) -> list[Member]:
    filter_no_ilvl = partial(filter_ilvl, min_ilvl = 0.0)
    filter_no_score = partial(filter_score, min_score = 0.0)
    postprocessor = compose(filter_no_score, filter_no_ilvl)
    return postprocessor(members)