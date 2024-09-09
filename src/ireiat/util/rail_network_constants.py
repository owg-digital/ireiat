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
    RAIL_TO_QUANT = "Rail Network to Quant Network Link"
    QUANT_TO_RAIL = "Quant Network to Rail Network Link"
