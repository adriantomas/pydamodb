"""Integration tests for complex model serialization with DynamoDB.

These tests verify that complex Pydantic models with various field types
serialize correctly to DynamoDB and deserialize back without data loss.
"""

from collections.abc import Generator
from datetime import date, datetime, timedelta, timezone
from decimal import Decimal
from uuid import uuid4

import pytest
from mypy_boto3_dynamodb.service_resource import Table

from pydamodb.models import PydamoConfig
from test.conftest import (
    ComplexConfig,
    ComplexEvent,
    ComplexOrder,
    ComplexProduct,
    ComplexTask,
    ComplexUser,
    ContactInfo,
    Dimensions,
    PaymentMethod,
    Priority,
    Status,
    create_sample_contact,
    create_sample_event,
    create_sample_order,
    create_sample_product,
    create_sample_task,
    create_sample_user,
)

# =============================================================================
# ComplexUser Serialization Tests
# =============================================================================


class TestComplexUserSerialization:
    """Test ComplexUser model serialization with DynamoDB."""

    def test_save_and_retrieve_minimal_user(self, user_table: Table) -> None:
        """Test saving a user with minimal required fields."""
        ComplexUser.pydamo_config = PydamoConfig(table=user_table)
        now = datetime.now(timezone.utc)

        user = ComplexUser(
            user_id="user-minimal",
            username="minimaluser",
            contact=ContactInfo(email="minimal@example.com"),
            registration_date=now,
            created_at=now,
        )
        user.save()

        retrieved = ComplexUser.get_item("user-minimal")
        assert retrieved is not None
        assert retrieved.user_id == "user-minimal"
        assert retrieved.username == "minimaluser"
        assert retrieved.contact.email == "minimal@example.com"
        assert retrieved.display_name is None
        assert retrieved.primary_address is None
        assert retrieved.tags == []
        assert retrieved.is_active is True

    def test_save_and_retrieve_full_user(self, user_table: Table) -> None:
        """Test saving a user with all fields populated."""
        ComplexUser.pydamo_config = PydamoConfig(table=user_table)

        user = create_sample_user("user-full", with_addresses=True)
        user.preferences = {
            "email_notifications": True,
            "dark_mode": False,
            "two_factor_enabled": True,
        }
        user.bio = "A passionate developer."
        user.date_of_birth = date(1990, 5, 15)
        user.last_login_at = datetime.now(timezone.utc)

        user.save()

        retrieved = ComplexUser.get_item("user-full")
        assert retrieved is not None

        # Basic fields
        assert retrieved.user_id == "user-full"
        assert retrieved.username == "johndoe"
        assert retrieved.display_name == "John Doe"
        assert retrieved.bio == "A passionate developer."

        # Nested ContactInfo
        assert retrieved.contact.email == "john@example.com"
        assert retrieved.contact.preferred_contact == "email"

        # Nested Address
        assert retrieved.primary_address is not None
        assert retrieved.primary_address.city == "Springfield"
        assert retrieved.primary_address.is_primary is True

        # List of addresses
        assert len(retrieved.additional_addresses) == 1
        assert retrieved.additional_addresses[0].city == "Chicago"

        # Lists
        assert "beta-tester" in retrieved.tags
        assert "technology" in retrieved.interests

        # Decimals
        assert retrieved.account_balance == Decimal("150.50")

        # Preferences dict
        assert retrieved.preferences["email_notifications"] is True
        assert retrieved.preferences["dark_mode"] is False

        # Dates
        assert retrieved.date_of_birth == date(1990, 5, 15)

    def test_update_nested_contact_info(self, user_table: Table) -> None:
        """Test updating nested model fields."""
        ComplexUser.pydamo_config = PydamoConfig(table=user_table)

        user = create_sample_user("user-update")
        user.save()

        # Update via save (full replace)
        user.contact = ContactInfo(
            email="newemail@example.com",
            phone="+1-555-999-8888",
            preferred_contact="phone",
        )
        user.save()

        retrieved = ComplexUser.get_item("user-update")
        assert retrieved is not None
        assert retrieved.contact.email == "newemail@example.com"
        assert retrieved.contact.phone == "+1-555-999-8888"
        assert retrieved.contact.preferred_contact == "phone"

    def test_empty_lists_preserved(self, user_table: Table) -> None:
        """Test that empty lists are preserved correctly."""
        ComplexUser.pydamo_config = PydamoConfig(table=user_table)
        now = datetime.now(timezone.utc)

        user = ComplexUser(
            user_id="user-empty-lists",
            username="emptylistuser",
            contact=create_sample_contact(),
            registration_date=now,
            created_at=now,
            tags=[],
            interests=[],
            additional_addresses=[],
        )
        user.save()

        retrieved = ComplexUser.get_item("user-empty-lists")
        assert retrieved is not None
        assert retrieved.tags == []
        assert retrieved.interests == []
        assert retrieved.additional_addresses == []


# =============================================================================
# ComplexProduct Serialization Tests
# =============================================================================


class TestComplexProductSerialization:
    """Test ComplexProduct model serialization with DynamoDB."""

    def test_save_and_retrieve_product_with_variants(self, product_table: Table) -> None:
        """Test saving a product with multiple variants."""
        ComplexProduct.pydamo_config = PydamoConfig(table=product_table)

        product = create_sample_product("prod-variants", with_variants=True)
        product.save()

        retrieved = ComplexProduct.get_item("prod-variants")
        assert retrieved is not None
        assert retrieved.name == "Wireless Headphones"
        assert len(retrieved.variants) == 3

        # Check variant details
        black_variant = next(v for v in retrieved.variants if v.color == "Black")
        assert black_variant.sku == "prod-variants-BLK"
        assert black_variant.stock_count == 30

        red_variant = next(v for v in retrieved.variants if v.color == "Red")
        assert red_variant.price_modifier == Decimal("10.00")

    def test_decimal_precision_preserved(self, product_table: Table) -> None:
        """Test that Decimal precision is preserved through serialization."""
        ComplexProduct.pydamo_config = PydamoConfig(table=product_table)
        now = datetime.now(timezone.utc)

        product = ComplexProduct(
            product_id="prod-decimals",
            name="Precision Test Product",
            slug="precision-test",
            category="Test",
            base_price=Decimal("123.456789"),
            sale_price=Decimal("99.99"),
            cost_price=Decimal("50.505050"),
            created_at=now,
        )
        product.save()

        retrieved = ComplexProduct.get_item("prod-decimals")
        assert retrieved is not None
        # Note: DynamoDB stores numbers with up to 38 digits of precision
        assert retrieved.base_price == Decimal("123.456789")
        assert retrieved.sale_price == Decimal("99.99")
        assert retrieved.cost_price == Decimal("50.505050")

    def test_dimensions_nested_model(self, product_table: Table) -> None:
        """Test nested Dimensions model serialization."""
        ComplexProduct.pydamo_config = PydamoConfig(table=product_table)
        now = datetime.now(timezone.utc)

        product = ComplexProduct(
            product_id="prod-dimensions",
            name="Boxed Item",
            slug="boxed-item",
            category="Shipping",
            base_price=Decimal("50.00"),
            dimensions=Dimensions(
                length_cm=Decimal("30.5"),
                width_cm=Decimal("20.25"),
                height_cm=Decimal("10.125"),
                weight_kg=Decimal("2.5"),
            ),
            created_at=now,
        )
        product.save()

        retrieved = ComplexProduct.get_item("prod-dimensions")
        assert retrieved is not None
        assert retrieved.dimensions is not None
        assert retrieved.dimensions.length_cm == Decimal("30.5")
        assert retrieved.dimensions.width_cm == Decimal("20.25")
        assert retrieved.dimensions.height_cm == Decimal("10.125")
        assert retrieved.dimensions.weight_kg == Decimal("2.5")

    def test_enum_serialization(self, product_table: Table) -> None:
        """Test Status enum serialization."""
        ComplexProduct.pydamo_config = PydamoConfig(table=product_table)

        product = create_sample_product("prod-enum")
        product.status = Status.COMPLETED
        product.save()

        retrieved = ComplexProduct.get_item("prod-enum")
        assert retrieved is not None
        assert retrieved.status == Status.COMPLETED
        assert retrieved.status.value == "completed"


# =============================================================================
# ComplexOrder Serialization Tests (Composite Key)
# =============================================================================


class TestComplexOrderSerialization:
    """Test ComplexOrder model serialization with composite keys."""

    def test_save_and_retrieve_order_with_line_items(self, order_table: Table) -> None:
        """Test saving an order with multiple line items."""
        ComplexOrder.pydamo_config = PydamoConfig(table=order_table)

        order = create_sample_order("cust-001", "ord-001")
        order.save()

        retrieved = ComplexOrder.get_item("cust-001", "ord-001")
        assert retrieved is not None
        assert retrieved.customer_id == "cust-001"
        assert retrieved.order_id == "ord-001"
        assert len(retrieved.line_items) == 2
        assert retrieved.item_count == 3  # 1 + 2 quantity

        # Check line item details
        headphones = next(i for i in retrieved.line_items if "Headphones" in i.product_name)
        assert headphones.quantity == 1
        assert headphones.unit_price == Decimal("99.99")

    def test_payment_details_nested(self, order_table: Table) -> None:
        """Test PaymentDetails nested model."""
        ComplexOrder.pydamo_config = PydamoConfig(table=order_table)

        order = create_sample_order("cust-002", "ord-002", with_payment=True)
        order.save()

        retrieved = ComplexOrder.get_item("cust-002", "ord-002")
        assert retrieved is not None
        assert retrieved.payment is not None
        assert retrieved.payment.method == PaymentMethod.CREDIT_CARD
        assert retrieved.payment.transaction_id == "txn-12345"
        assert retrieved.payment.amount.currency == "USD"
        assert retrieved.is_paid is True

    def test_shipping_info_with_address(self, order_table: Table) -> None:
        """Test ShippingInfo with nested Address."""
        ComplexOrder.pydamo_config = PydamoConfig(table=order_table)

        order = create_sample_order("cust-003", "ord-003", with_shipping=True)
        order.save()

        retrieved = ComplexOrder.get_item("cust-003", "ord-003")
        assert retrieved is not None
        assert retrieved.shipping is not None
        assert retrieved.shipping.carrier == "UPS"
        assert retrieved.shipping.address.city == "Springfield"
        assert retrieved.shipping.estimated_delivery is not None

    def test_query_orders_by_customer(self, order_table: Table) -> None:
        """Test querying multiple orders for a customer."""
        ComplexOrder.pydamo_config = PydamoConfig(table=order_table)

        # Create multiple orders for same customer
        for i in range(5):
            order = create_sample_order("cust-query", f"ord-{i:03d}")
            order.save()

        # Query all orders
        result = ComplexOrder.query("cust-query")
        assert len(result.items) == 5

        # Verify order IDs
        order_ids = [o.order_id for o in result.items]
        assert "ord-000" in order_ids
        assert "ord-004" in order_ids

    def test_priority_int_enum(self, order_table: Table) -> None:
        """Test Priority IntEnum serialization."""
        ComplexOrder.pydamo_config = PydamoConfig(table=order_table)

        order = create_sample_order("cust-priority", "ord-priority")
        order.priority = Priority.CRITICAL
        order.save()

        retrieved = ComplexOrder.get_item("cust-priority", "ord-priority")
        assert retrieved is not None
        assert retrieved.priority == Priority.CRITICAL
        assert retrieved.priority.value == 4


# =============================================================================
# ComplexEvent Serialization Tests (Time-series)
# =============================================================================


class TestComplexEventSerialization:
    """Test ComplexEvent model for time-series/audit data."""

    def test_uuid_serialization(self, event_table: Table) -> None:
        """Test UUID field serialization."""
        ComplexEvent.pydamo_config = PydamoConfig(table=event_table)

        event_id = uuid4()
        correlation_id = uuid4()

        event = create_sample_event("entity-uuid", event_id=event_id)
        event.correlation_id = correlation_id
        event.save()

        retrieved = ComplexEvent.get_item("entity-uuid", event.event_timestamp)
        assert retrieved is not None
        assert retrieved.event_id == event_id
        assert retrieved.correlation_id == correlation_id

    def test_geo_location_nested(self, event_table: Table) -> None:
        """Test GeoLocation nested model with Decimal coordinates."""
        ComplexEvent.pydamo_config = PydamoConfig(table=event_table)

        event = create_sample_event("entity-geo")
        event.save()

        retrieved = ComplexEvent.get_item("entity-geo", event.event_timestamp)
        assert retrieved is not None
        assert retrieved.location is not None
        assert retrieved.location.latitude == Decimal("41.8781")
        assert retrieved.location.longitude == Decimal("-87.6298")

    def test_flexible_payload_dict(self, event_table: Table) -> None:
        """Test flexible payload dictionary with mixed types."""
        ComplexEvent.pydamo_config = PydamoConfig(table=event_table)
        now = datetime.now(timezone.utc)

        event = ComplexEvent(
            entity_id="entity-payload",
            event_timestamp=now.isoformat(),
            event_id=uuid4(),
            event_type="custom.event",
            payload={
                "string_value": "hello",
                "int_value": 42,
                "decimal_value": Decimal("3.14"),  # DynamoDB requires Decimal, not float
                "bool_value": True,
                "null_value": None,
            },
            source_system="test-system",
        )
        event.save()

        retrieved = ComplexEvent.get_item("entity-payload", event.event_timestamp)
        assert retrieved is not None
        assert retrieved.payload["string_value"] == "hello"
        assert retrieved.payload["int_value"] == 42
        # Note: Decimal in untyped dict gets serialized to string by Pydantic
        # For precise decimal handling, use typed fields on the model
        assert retrieved.payload["decimal_value"] == "3.14"
        assert retrieved.payload["bool_value"] is True
        assert retrieved.payload["null_value"] is None

    def test_event_time_series_query(self, event_table: Table) -> None:
        """Test querying events in chronological order."""
        ComplexEvent.pydamo_config = PydamoConfig(table=event_table)

        base_time = datetime.now(timezone.utc)
        entity_id = "entity-timeseries"

        # Create events at different timestamps
        for i in range(10):
            event_time = base_time + timedelta(minutes=i)
            event = ComplexEvent(
                entity_id=entity_id,
                event_timestamp=event_time.isoformat(),
                event_id=uuid4(),
                event_type=f"event.type.{i}",
                source_system="test",
            )
            event.save()

        # Query should return in timestamp order
        result = ComplexEvent.query(entity_id)
        assert len(result.items) == 10

        # Verify chronological order (ascending by default)
        timestamps = [e.event_timestamp for e in result.items]
        assert timestamps == sorted(timestamps)


# =============================================================================
# ComplexTask Serialization Tests
# =============================================================================


class TestComplexTaskSerialization:
    """Test ComplexTask model with audit metadata."""

    def test_save_and_retrieve_task(self, task_table: Table) -> None:
        """Test basic task serialization."""
        ComplexTask.pydamo_config = PydamoConfig(table=task_table)

        task = create_sample_task("proj-001", "task-001")
        task.save()

        retrieved = ComplexTask.get_item("proj-001", "task-001")
        assert retrieved is not None
        assert retrieved.title == "Implement feature X"
        assert retrieved.status == Status.PENDING
        assert retrieved.priority == Priority.HIGH

    def test_audit_metadata_nested(self, task_table: Table) -> None:
        """Test AuditMetadata nested model."""
        ComplexTask.pydamo_config = PydamoConfig(table=task_table)

        task = create_sample_task("proj-audit", "task-audit")
        task.save()

        retrieved = ComplexTask.get_item("proj-audit", "task-audit")
        assert retrieved is not None
        assert retrieved.audit.created_by == "user-001"
        assert retrieved.audit.version == 1
        assert retrieved.audit.updated_at is None

    def test_lists_of_strings(self, task_table: Table) -> None:
        """Test multiple list[str] fields."""
        ComplexTask.pydamo_config = PydamoConfig(table=task_table)

        task = create_sample_task("proj-lists", "task-lists")
        task.subtask_ids = ["subtask-1", "subtask-2", "subtask-3"]
        task.attachment_ids = ["attach-1", "attach-2"]
        task.related_task_ids = ["related-1"]
        task.external_links = ["https://example.com/doc1", "https://example.com/doc2"]
        task.save()

        retrieved = ComplexTask.get_item("proj-lists", "task-lists")
        assert retrieved is not None
        assert len(retrieved.subtask_ids) == 3
        assert len(retrieved.attachment_ids) == 2
        assert len(retrieved.related_task_ids) == 1
        assert len(retrieved.external_links) == 2

    def test_date_fields(self, task_table: Table) -> None:
        """Test date field serialization."""
        ComplexTask.pydamo_config = PydamoConfig(table=task_table)

        task = create_sample_task("proj-dates", "task-dates")
        task.start_date = date(2024, 1, 15)
        task.due_date = date(2024, 1, 22)
        task.completed_date = date(2024, 1, 20)
        task.save()

        retrieved = ComplexTask.get_item("proj-dates", "task-dates")
        assert retrieved is not None
        assert retrieved.start_date == date(2024, 1, 15)
        assert retrieved.due_date == date(2024, 1, 22)
        assert retrieved.completed_date == date(2024, 1, 20)

    def test_decimal_hours_tracking(self, task_table: Table) -> None:
        """Test Decimal fields for time tracking."""
        ComplexTask.pydamo_config = PydamoConfig(table=task_table)

        task = create_sample_task("proj-hours", "task-hours")
        task.estimated_hours = Decimal("16.5")
        task.logged_hours = Decimal("8.25")
        task.save()

        retrieved = ComplexTask.get_item("proj-hours", "task-hours")
        assert retrieved is not None
        assert retrieved.estimated_hours == Decimal("16.5")
        assert retrieved.logged_hours == Decimal("8.25")


# =============================================================================
# ComplexConfig Serialization Tests (Binary data)
# =============================================================================


class TestComplexConfigSerialization:
    """Test ComplexConfig model with various primitive types."""

    @pytest.fixture
    def config_table(self) -> Generator[Table, None, None]:
        """Create a table for config models."""
        import boto3
        from moto import mock_aws

        with mock_aws():
            dynamodb = boto3.resource("dynamodb")
            table = dynamodb.create_table(
                TableName="Configs",
                KeySchema=[
                    {"AttributeName": "config_key", "KeyType": "HASH"},
                ],
                AttributeDefinitions=[
                    {"AttributeName": "config_key", "AttributeType": "S"},
                ],
                BillingMode="PAY_PER_REQUEST",
            )
            table.wait_until_exists()
            yield table
            table.delete()

    def test_feature_flags_dict(self, config_table: Table) -> None:
        """Test dictionary of booleans."""
        ComplexConfig.pydamo_config = PydamoConfig(table=config_table)
        now = datetime.now(timezone.utc)

        config = ComplexConfig(
            config_key="app-config-flags",
            environment="production",
            feature_flags={
                "new_dashboard": True,
                "beta_api": False,
                "maintenance_mode": False,
                "enable_analytics": True,
            },
            last_modified=now,
        )
        config.save()

        retrieved = ComplexConfig.get_item("app-config-flags")
        assert retrieved is not None
        assert retrieved.feature_flags["new_dashboard"] is True
        assert retrieved.feature_flags["beta_api"] is False
        assert len(retrieved.feature_flags) == 4

    def test_rate_limits_dict(self, config_table: Table) -> None:
        """Test dictionary of integers."""
        ComplexConfig.pydamo_config = PydamoConfig(table=config_table)
        now = datetime.now(timezone.utc)

        config = ComplexConfig(
            config_key="app-config-rates",
            environment="staging",
            rate_limits={
                "api_calls_per_minute": 100,
                "max_connections": 50,
                "request_timeout_ms": 30000,
            },
            last_modified=now,
        )
        config.save()

        retrieved = ComplexConfig.get_item("app-config-rates")
        assert retrieved is not None
        assert retrieved.rate_limits["api_calls_per_minute"] == 100
        assert retrieved.rate_limits["max_connections"] == 50

    def test_thresholds_decimal_dict(self, config_table: Table) -> None:
        """Test dictionary of Decimals."""
        ComplexConfig.pydamo_config = PydamoConfig(table=config_table)
        now = datetime.now(timezone.utc)

        config = ComplexConfig(
            config_key="app-config-thresholds",
            environment="development",
            thresholds={
                "error_rate_percent": Decimal("5.5"),
                "latency_p99_ms": Decimal("250.75"),
                "cpu_usage_max": Decimal("85.0"),
            },
            last_modified=now,
        )
        config.save()

        retrieved = ComplexConfig.get_item("app-config-thresholds")
        assert retrieved is not None
        assert retrieved.thresholds["error_rate_percent"] == Decimal("5.5")
        assert retrieved.thresholds["latency_p99_ms"] == Decimal("250.75")

    def test_binary_data_serialization(self, config_table: Table) -> None:
        """Test bytes field serialization.

        Note: This test verifies that binary data can be stored and retrieved.
        DynamoDB stores binary data as base64-encoded strings in JSON mode.
        """
        ComplexConfig.pydamo_config = PydamoConfig(table=config_table)
        now = datetime.now(timezone.utc)

        # Sample binary data (could be encryption key, certificate, etc.)
        encryption_key = b"\x00\x01\x02\x03\x04\x05\x06\x07\x08\x09\x0a\x0b\x0c\x0d\x0e\x0f"
        certificate_data = b"-----BEGIN CERTIFICATE-----\nMIIBkTCB+..."

        config = ComplexConfig(
            config_key="app-config-binary",
            environment="production",
            encryption_key=encryption_key,
            certificate_data=certificate_data,
            last_modified=now,
        )
        config.save()

        retrieved = ComplexConfig.get_item("app-config-binary")
        assert retrieved is not None
        # Pydantic serializes bytes as base64, verify we get back equivalent data
        assert retrieved.encryption_key == encryption_key
        assert retrieved.certificate_data == certificate_data

    def test_mixed_type_dict(self, config_table: Table) -> None:
        """Test dictionary with mixed value types."""
        ComplexConfig.pydamo_config = PydamoConfig(table=config_table)
        now = datetime.now(timezone.utc)

        config = ComplexConfig(
            config_key="app-config-mixed",
            environment="staging",
            email_settings={
                "smtp_host": "smtp.example.com",
                "smtp_port": 587,
                "use_tls": True,
            },
            last_modified=now,
        )
        config.save()

        retrieved = ComplexConfig.get_item("app-config-mixed")
        assert retrieved is not None
        assert retrieved.email_settings["smtp_host"] == "smtp.example.com"
        assert retrieved.email_settings["smtp_port"] == 587
        assert retrieved.email_settings["use_tls"] is True

    def test_literal_type_validation(self, config_table: Table) -> None:
        """Test Literal type field."""
        ComplexConfig.pydamo_config = PydamoConfig(table=config_table)
        now = datetime.now(timezone.utc)

        for env in ["development", "staging", "production"]:
            config = ComplexConfig(
                config_key=f"config-{env}",
                environment=env,
                last_modified=now,
            )
            config.save()

            retrieved = ComplexConfig.get_item(f"config-{env}")
            assert retrieved is not None
            assert retrieved.environment == env
