from __future__ import annotations

from connectors.base import ConnectorSpec

CONNECTOR_REGISTRY = {
    "quickbooks": ConnectorSpec(
        name="quickbooks",
        auth_mode="oauth2",
        entities=["invoices", "customers", "payments"],
    ),
    "shopify": ConnectorSpec(
        name="shopify",
        auth_mode="oauth2",
        entities=["orders", "customers", "refunds", "products"],
    ),
    "hubspot": ConnectorSpec(
        name="hubspot",
        auth_mode="oauth2",
        entities=["contacts", "companies", "deals", "tickets"],
    ),
    "stripe": ConnectorSpec(
        name="stripe",
        auth_mode="api_key",
        entities=["charges", "customers", "invoices", "refunds", "disputes", "payment_intents", "balance_transactions"],
    ),
}
