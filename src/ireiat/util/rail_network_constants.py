from enum import StrEnum


class VertexType(StrEnum):
    """
    Enum for vertex types in the rail network graph.
    """

    IM_TERMINAL = "Intermodal Terminal"
    IM_DUMMY_NODE = "Intermodal Dummy Node"


class EdgeType(StrEnum):
    """
    Enum for edge types in the rail network graph.
    """

    IM_CAPACITY = "Intermodal Capacity Link"
    IM_DUMMY = "Linkage between IM and rail"
    RAIL_LINK = "Rail Link"
    IMPEDANCE_LINK = "Impedance Link"
