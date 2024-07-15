import geopandas
import pandas as pd
from shapely.geometry import LineString


class RailNode:
    def __init__(
        self,
        node_id: int,
        latitude: float,
        longitude: float,
        railroads: set[str] = None,
        link_railroads: dict[int, set[str]] = None,
    ):
        """
        Represents a node in the rail network.

        Args:
            node_id (int): FRA node identifier.
            latitude (float): Latitude of the rail node.
            longitude (float): Longitude of the rail node.
            railroads (set[str]): Set of railroads touching this node.
            link_railroads (dict[int, set[str]]): Dictionary mapping link IDs to their respective railroads.
        """
        self.node_id = node_id
        self.latitude = latitude
        self.longitude = longitude
        self.railroads = railroads if railroads is not None else set()
        self.link_railroads = link_railroads if link_railroads is not None else {}

    def __repr__(self) -> str:
        return f"RailNode(node_id={self.node_id}, railroads={self.railroads}, link_railroads={self.link_railroads})"

    def add_railroads_and_links(self, link_id: int, new_railroads: set[str]):
        """
        Adds new railroads to the node's railroads set and updates the link_railroads dictionary.

        Args:
            link_id (int): The ID of the link.
            new_railroads (set[str]): Set of railroads associated with the link.
        """
        # Ensure we only add railroads that are part of the new_railroads set to self.railroads
        updated_railroads = self.railroads.copy()
        updated_railroads.update(new_railroads)
        self.railroads = updated_railroads

        # Use copy to avoid mutating the original set
        self.link_railroads[link_id] = new_railroads.copy()


class QuantNode:
    def __init__(self, rail_node: RailNode, railroad: str):
        """
        Represents a QuantNode in the rail network, specific to a railroad.

        Args:
            rail_node (RailNode): An existing RailNode object to associate with.
            railroad (str): Specific railroad for the QuantNode.
        """
        self.rail_node = rail_node
        self.railroad = railroad
        assert self.is_specific_railroad(), f"Railroad {railroad} is not part of the rail node's railroads."

    def __repr__(self) -> str:
        return f"QuantNode(node_id={self.rail_node.node_id}, railroad={self.railroad})"

    def is_specific_railroad(self) -> bool:
        return self.railroad in self.rail_node.railroads

    
class RailLink:
    def __init__(
        self,
        link_id: int,
        from_node: RailNode,
        to_node: RailNode,
        state_county_fips: str,
        state: str,
        country: str,
        yard_name: str,
        passenger: str,
        net: str,
        miles: float,
        geometry: LineString,
        owners: set[str],
        track_rights: set[str],
        railroads: set[str],
    ):
        """
        Represents a link in the rail network.

        Args:
            link_id (int): Unique identifier for the rail link.
            from_node (RailNode): The originating rail node.
            to_node (RailNode): The destination rail node.
            state_county_fips (str): Combined state and county FIPS code.
            state (str): State where the rail link is located.
            country (str): Country where the rail link is located.
            yard_name (str): Yard name.
            passenger (str): Passenger information.
            net (str): Network information.
            miles (float): Distance in miles.
            geometry (LineString): Geometric representation of the rail link as a LineString object.
            owners (set[str]): Set of owners.
            track_rights (set[str]): Set of track rights.
            railroads (set[str]): Set of railroads.
        """
        self.link_id = link_id
        self.from_node = from_node
        self.to_node = to_node
        self.state_county_fips = state_county_fips
        self.state = state
        self.country = country
        self.yard_name = yard_name
        self.passenger = passenger
        self.net = net
        self.miles = miles
        self.geometry = geometry
        self.owners = owners
        self.track_rights = track_rights
        self.railroads = railroads

    def __repr__(self) -> str:
        return (
            f"RailLink(link_id={self.link_id}, from_node={self.from_node.node_id}, to_node={self.to_node.node_id}, "
            f"miles={self.miles}, owners={self.owners}, track_rights={self.track_rights}, railroads={self.railroads})"
        )


class QuantLink:
    def __init__(self, rail_link: RailLink, from_node: QuantNode, to_node: QuantNode, railroad: str):
        """
        Represents a QuantLink in the rail network, specific to a railroad.

        Args:
            rail_link (RailLink): An existing RailLink object to inherit from.
            from_node (QuantNode): The originating QuantNode.
            to_node (QuantNode): The destination QuantNode.
            railroad (str): Specific railroad for the QuantLink.
        """
        self.rail_link = rail_link
        self.from_node = from_node
        self.to_node = to_node
        self.railroad = railroad

        assert self.is_specific_railroad(), (
            f"Railroad {railroad} is not part of the rail link's railroads or does not match from/to nodes."
        )

    def __repr__(self) -> str:
        return (
            f"QuantLink(link_id={self.rail_link.link_id}, from_node={self.from_node.node_id}, "
            f"to_node={self.to_node.node_id}, railroad={self.railroad})"
        )

    def is_specific_railroad(self) -> bool:
        """
        Check if the link is specific to the given railroad and if both nodes belong to the same railroad.

        Returns:
            bool: True if the link belongs to the specific railroad and both nodes belong to the same railroad, False otherwise.
        """
        return (
            self.railroad in self.rail_link.railroads
            and self.from_node.railroad == self.to_node.railroad == self.railroad
        )

    
class IntermodalTerminal:
    def __init__(
        self,
        id: int,
        terminal_name: str,
        railroads: set[str],
        rail_node: RailNode,
        yard_name: str,
        capacity: float = None,
    ):
        """
        Represents an intermodal terminal in the rail network.

        Args:
            id (int): Unique identifier for the terminal.
            terminal_name (str): Name of the terminal.
            railroads (set[str]): Set of railroads associated with the terminal.
            rail_node (RailNode): Rail node where the terminal is located.
            yard_name (str): Name of the yard.
            capacity (float, optional): Capacity of the terminal. Defaults to None.
        """
        self.id = id
        self.terminal_name = terminal_name
        self.railroads = railroads
        self.rail_node = rail_node
        self.yard_name = yard_name
        self.capacity = capacity

    def __repr__(self) -> str:
        return (
            f"IntermodalTerminal(id={self.id}, terminal_name='{self.terminal_name}', rail_node={self.rail_node}, "
            f"yard_name='{self.yard_name}', capacity={self.capacity})"
        )


class RailNetwork:
    def __init__(self):
        """
        Represents a rail network consisting of various types of nodes and links.
        """
        self.rail_nodes: dict[int, RailNode] = {}
        self.rail_links: dict[int, RailLink] = {}
        self.quant_nodes: dict[tuple[int, str], QuantNode] = {}
        self.quant_links: dict[tuple[int, str], QuantLink] = {}
        self.intermodal_terminals: dict[tuple[str, int], IntermodalTerminal] = {}

    def __repr__(self) -> str:
        return (
            f"RailNetwork(nodes={len(self.rail_nodes)}, links={len(self.rail_links)}, "
            f"quant_nodes={len(self.quant_nodes)}, quant_links={len(self.quant_links)}, "
            f"terminals={len(self.intermodal_terminals)})"
        )

    def add_rail_nodes_and_links(self, links_df: geopandas.GeoDataFrame):
        """
        Adds rail nodes and links to the rail network from a DataFrame.

        Args:
            links_df (geopandas.GeoDataFrame): DataFrame containing link data with columns for nodes and links.
        """
        for row in links_df.itertuples(index=False):
            # Extract necessary information from the DataFrame row
            from_node_id = row.from_node
            to_node_id = row.to_node
            railroads = set(row.railroads)
            link_id = row.link_id

            # Create or update the from_node
            if from_node_id not in self.rail_nodes:
                from_node_lat = row.geometry.coords[0][1]
                from_node_lon = row.geometry.coords[0][0]
                from_node = RailNode(
                    from_node_id, from_node_lat, from_node_lon, railroads
                )
                self.rail_nodes[from_node_id] = from_node

            # Add link railroads to from_node
            self.rail_nodes[from_node_id].add_railroads_and_links(link_id, railroads)

            ## Create or update the to_node
            if to_node_id not in self.rail_nodes:
                to_node_lat = row.geometry.coords[-1][1]
                to_node_lon = row.geometry.coords[-1][0]
                to_node = RailNode(to_node_id, to_node_lat, to_node_lon, railroads)
                self.rail_nodes[to_node_id] = to_node

            # Add link railroads to to_node
            self.rail_nodes[to_node_id].add_railroads_and_links(link_id, railroads)

            # Create the RailLink object
            rail_link = RailLink(
                link_id=link_id,
                from_node=self.rail_nodes[from_node_id],
                to_node=self.rail_nodes[to_node_id],
                state_county_fips=row.state_county_fips,
                state=row.state,
                country=row.country,
                yard_name=row.yard_name,
                passenger=row.passenger,
                net=row.net,
                miles=row.distance,
                geometry=row.geometry,
                owners=row.owners,
                track_rights=row.track_rights,
                railroads=row.railroads,
            )
            self.rail_links[link_id] = rail_link

    def add_quant_nodes_and_links(self):
        """
        Adds quant nodes and quant links to the rail network.
        """
        # Create QuantNodes
        for node_id, node in self.rail_nodes.items():
            for railroad in node.railroads:
                quant_node_key = (node_id, railroad)
                self.quant_nodes[quant_node_key] = QuantNode(node, railroad)

        # Create QuantLinks
        for link_id, link in self.rail_links.items():
            for railroad in link.railroads:
                quant_from_node_key = (link.from_node.node_id, railroad)
                quant_to_node_key = (link.to_node.node_id, railroad)

                # Ensure both quant nodes exist
                if quant_from_node_key in self.quant_nodes and quant_to_node_key in self.quant_nodes:
                    quant_link_key = (link_id, railroad)
                    quant_link = QuantLink(
                        rail_link=link,
                        from_node=self.quant_nodes[quant_from_node_key],
                        to_node=self.quant_nodes[quant_to_node_key],
                        railroad=railroad,
                    )
                    self.quant_links[quant_link_key] = quant_link

    def add_intermodal_terminals(self, terminals_df: pd.DataFrame):
        """
        Adds intermodal terminals to the network from a DataFrame.

        Args:
            terminals_df (pd.DataFrame): DataFrame containing terminal data.
        """
        for row in terminals_df.itertuples(index=False):
            rail_node = self.rail_nodes[row.rail_node]

            terminal = IntermodalTerminal(
                id=row.id,
                terminal_name=row.terminal_name,
                railroads=row.railroads,
                rail_node=rail_node,
                yard_name=row.yard_name,
                capacity=getattr(row, "capacity", None),
            )
            intermodal_terminal_key = (
                terminal.terminal_name,
                terminal.rail_node.node_id,
            )
            self.intermodal_terminals[intermodal_terminal_key] = terminal

    def build_railroad_network(
        self, links_df: geopandas.GeoDataFrame, terminals_df: pd.DataFrame
    ):
        """
        Builds the railroad network by executing the sequence of methods.

        Args:
            links_df (pd.DataFrame): DataFrame containing link data.
            terminals_df (pd.DataFrame): DataFrame containing terminal data.
        """
        # Add rail nodes and links
        self.add_rail_nodes_and_links(links_df=links_df)

        # Add quant nodes and links
        self.add_quant_nodes_and_links()

        # Add intermodal terminals
        self.add_intermodal_terminals(terminals_df=terminals_df)