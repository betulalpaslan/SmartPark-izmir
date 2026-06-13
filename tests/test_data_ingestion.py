import unittest

from tests.helpers import install_data_ingestion_stubs, load_module


class DataIngestionNormalizeTests(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        install_data_ingestion_stubs()
        cls.ingestion = load_module("data_ingestion_main", "data-ingestion/main.py")

    def test_derives_occupied_from_free_and_capacity(self):
        lot = self.ingestion.normalize(
            {
                "ufid": "demo-1",
                "Isim": "Plevne Bulvari",
                "Kapasite": 100,
                "BosKapasite": 40,
                "Enlem": "38,4",
                "Boylam": "27,1",
            }
        )

        self.assertIsNotNone(lot)
        self.assertEqual(lot["capacity"], 100)
        self.assertEqual(lot["free"], 40)
        self.assertEqual(lot["occupied"], 60)
        self.assertEqual(lot["occupancy_pct"], 60.0)
        self.assertEqual(lot["lat"], 38.4)
        self.assertEqual(lot["lng"], 27.1)

    def test_preserves_zero_free_spaces(self):
        lot = self.ingestion.normalize(
            {
                "id": "full-lot",
                "name": "Full Lot",
                "capacity": 25,
                "emptyCapacity": 0,
                "lat": 38.42,
                "lng": 27.13,
            }
        )

        self.assertIsNotNone(lot)
        self.assertEqual(lot["free"], 0)
        self.assertEqual(lot["occupied"], 25)
        self.assertEqual(lot["occupancy_pct"], 100.0)

    def test_derives_free_from_occupied_and_capacity(self):
        lot = self.ingestion.normalize(
            {
                "OtoparkId": "nested",
                "occupancy": {"total": {"occupied": 15, "capacity": 40}},
                "lat": 38.42,
                "lng": 27.13,
            }
        )

        self.assertIsNotNone(lot)
        self.assertEqual(lot["capacity"], 40)
        self.assertEqual(lot["free"], 25)
        self.assertEqual(lot["occupied"], 15)
        self.assertEqual(lot["occupancy_pct"], 37.5)

    def test_rejects_lot_without_capacity(self):
        self.assertIsNone(self.ingestion.normalize({"id": "bad", "BosKapasite": 0}))


if __name__ == "__main__":
    unittest.main()
