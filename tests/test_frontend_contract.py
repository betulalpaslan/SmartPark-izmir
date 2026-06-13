import unittest
from pathlib import Path


FRONTEND_HTML = Path(__file__).resolve().parents[1] / "frontend" / "index.html"


class FrontendContractTests(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.html = FRONTEND_HTML.read_text(encoding="utf-8")

    def test_user_location_control_exists(self):
        self.assertIn("Konumumu Kullan", self.html)
        self.assertIn('id="user-meta"', self.html)
        self.assertIn("navigator.geolocation.getCurrentPosition", self.html)

    def test_recommendation_request_uses_user_coordinates(self):
        self.assertIn("const startCoords = userCoords || await locateUser() || destCoords", self.html)
        self.assertIn("userLat: startCoords.lat", self.html)
        self.assertIn("userLng: startCoords.lng", self.html)
        self.assertIn("destLat: destCoords.lat", self.html)
        self.assertIn("destLng: destCoords.lng", self.html)


if __name__ == "__main__":
    unittest.main()
