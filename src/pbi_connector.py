"""
pbi_connector.py — Power BI REST API connector (placeholder).

Requires:
  - Azure Entra (AD) app registration with Power BI API permissions
  - MSAL library for authentication
  - Dataset with inventory table

See docs/powerbi_rest.md for setup instructions.
"""

from __future__ import annotations


class PowerBIConnector:
    """Placeholder Power BI connector. Not yet functional."""

    def __init__(self, tenant_id: str, client_id: str, dataset_id: str, table_name: str = "Inventory"):
        self.tenant_id = tenant_id
        self.client_id = client_id
        self.dataset_id = dataset_id
        self.table_name = table_name
        self._token = None

    def authenticate(self, username: str, password: str) -> bool:
        """
        Authenticate with Azure AD using MSAL interactive login.

        TODO: Implement using msal library:
            import msal
            app = msal.PublicClientApplication(self.client_id, authority=f"https://login.microsoftonline.com/{self.tenant_id}")
            result = app.acquire_token_by_username_password(username, password, scopes=["https://analysis.windows.net/powerbi/api/.default"])
            self._token = result.get("access_token")
        """
        raise NotImplementedError(
            "Power BI authentication not yet implemented. "
            "See docs/powerbi_rest.md for setup instructions."
        )

    def query_availability(self, part_number: str) -> list[dict]:
        """
        Query inventory availability from Power BI dataset.

        TODO: Implement using Power BI REST API:
            import requests
            headers = {"Authorization": f"Bearer {self._token}"}
            dax_query = f'EVALUATE FILTER({self.table_name}, {self.table_name}[PartNumber] = "{part_number}")'
            url = f"https://api.powerbi.com/v1.0/myorg/datasets/{self.dataset_id}/executeQueries"
            payload = {"queries": [{"query": dax_query}], "serializerSettings": {"includeNulls": True}}
            resp = requests.post(url, headers=headers, json=payload)
            return resp.json()["results"][0]["tables"][0]["rows"]

        Returns list of dicts with keys: part_number, on_hand, backorder, eta, location
        """
        if not self._token:
            raise RuntimeError("Not authenticated. Call authenticate() first.")
        raise NotImplementedError("Power BI query not yet implemented.")
