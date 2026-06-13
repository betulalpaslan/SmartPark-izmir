import unittest

from tests.helpers import install_fastapi_service_stubs, load_module


class PricingLogicTests(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        install_fastapi_service_stubs()
        cls.pricing = load_module("pricing_routing_main", "pricing-routing/main.py")

    def test_low_occupancy_gets_discount_multiplier(self):
        self.assertEqual(self.pricing.compute_multiplier(30, 30, 12), 0.8)

    def test_peak_hour_and_upward_trend_increase_multiplier(self):
        self.assertEqual(self.pricing.compute_multiplier(80, 90, 18), 1.36)

    def test_multiplier_is_capped(self):
        self.assertEqual(self.pricing.compute_multiplier(98, 100, 18), 1.8)

    def test_haversine_returns_zero_for_same_coordinate(self):
        self.assertEqual(self.pricing.haversine(38.4192, 27.1287, 38.4192, 27.1287), 0)

    def test_haversine_distance_is_reasonable_for_nearby_izmir_points(self):
        distance = self.pricing.haversine(38.4192, 27.1287, 38.4237, 27.1428)
        self.assertGreater(distance, 1200)
        self.assertLess(distance, 1400)


if __name__ == "__main__":
    unittest.main()
