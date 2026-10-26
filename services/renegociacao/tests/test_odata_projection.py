import unittest
from unittest.mock import Mock

from renegociacao.core import ApiError
from renegociacao.odata import read


class ODataProjection(unittest.TestCase):
    def setUp(self):
        self.identifier = "11111111-1111-4111-8111-111111111111"
        self.repository = Mock()
        self.repository.odata.return_value = [
            {"Id": self.identifier, "Name": "DEMO-001"}
        ]

    def test_dataverse_provider_selects_only_requested_columns_and_key(self):
        response, _ = read(
            self.repository, "Contracts", {"$select": "Id,Name", "$top": "1"},
            "https://source.example/api/odata",
        )
        self.assertEqual(response["value"], [{"Id": self.identifier, "Name": "DEMO-001"}])
        self.assertNotIn("@odata.nextLink", response)

    def test_single_entity_select_keeps_stable_key(self):
        response, _ = read(
            self.repository, f"Contracts({self.identifier})", {"$select": "Name"},
            "https://source.example/api/odata",
        )
        self.assertEqual(response["Id"], self.identifier)
        self.assertEqual(response["Name"], "DEMO-001")

    def test_invalid_select_is_explicitly_rejected_before_sql(self):
        for selected in ["", "Id,Name;DROP", "Unknown", "Id,Id", "*"]:
            with self.subTest(selected=selected), self.assertRaises(ApiError):
                read(self.repository, "Contracts", {"$select": selected}, "https://source.example/api/odata")
        self.repository.odata.assert_not_called()

    def test_explicit_native_provider_window_is_not_silently_cut_at_200(self):
        self.repository.odata.return_value = [
            {"Id": self.identifier, "Name": str(number)} for number in range(500)
        ]
        response, _ = read(
            self.repository, "Contracts", {"$select": "Id,Name", "$top": "500"},
            "https://source.example/api/odata",
        )
        self.assertEqual(len(response["value"]), 500)
        self.assertNotIn("@odata.nextLink", response)
        self.repository.odata.reset_mock()
        with self.assertRaises(ApiError) as error:
            read(self.repository, "Contracts", {"$top": "1001"}, "https://source.example/api/odata")
        self.assertEqual(error.exception.code, "ODATA_QUERY_LIMIT")
        self.repository.odata.assert_not_called()


if __name__ == "__main__":
    unittest.main()
