from enum import Enum


class RuntimeEnv(Enum):
    LOCAL = "local"
    CONTAINER = "container"


class Namespace(Enum):
    ACC = "icarus-acc"
    PRD = "icarus-prd"
