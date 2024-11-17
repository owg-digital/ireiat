import unittest

from ireiat.config.data_pipeline import RailImpedanceConfig
from ireiat.data_pipeline.assets.rail_network.impedance import (
    generate_impedance_values,
)


class TestImpedanceOverrides(unittest.TestCase):

    def setUp(self) -> None:
        self.config = RailImpedanceConfig(
            **{
                "class_1_rr_codes": ["CSX", "BNSF"],
                "class_1_to_class_1_impedance": 10,
                "default_impedance": 1,
            }
        )

        self.c1_to_c1_impedance = ("CSX", (1, 1), "BNSF", (2, 2))
        self.short_to_c1_impedance = ("some_shortline", (1, 1), "BNSF", (2, 2))
        self.c1_to_short_impedance = ("CSX", (1, 1), "some_shortline", (2, 2))
        self.short_to_short_impedance = ("other_shortline", (1, 1), "some_shortline", (2, 2))

    def test_impedance_overrides_computes_correctly(self):
        # c1 -> c1
        result = generate_impedance_values({self.c1_to_c1_impedance}, self.config)
        self.assertEqual(result[0], self.config.class_1_to_class_1_impedance)

        # c1 -> shortline
        result = generate_impedance_values({self.c1_to_short_impedance}, self.config)
        self.assertEqual(result[0], self.config.default_impedance)

        # shortline -> c1
        result = generate_impedance_values({self.short_to_c1_impedance}, self.config)
        self.assertEqual(result[0], self.config.default_impedance)

        # shortline -> shortline
        result = generate_impedance_values({self.short_to_short_impedance}, self.config)
        self.assertEqual(result[0], self.config.default_impedance)
