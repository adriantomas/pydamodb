"""Shared test fixtures and complex model definitions.

This module provides:
- Complex model definitions for testing serialization with DynamoDB
- Nested Pydantic models to verify recursive serialization
- Various Python types to test DynamoDB compatibility
- Reusable fixtures for all test modules

DynamoDB supported types (via boto3):
- String (S)
- Number (N) - stored as Decimal
- Binary (B) - bytes/bytearray
- Boolean (BOOL)
- Null (NULL)
- List (L) - heterogeneous lists
- Map (M) - nested objects
- String Set (SS)
- Number Set (NS)
- Binary Set (BS)
"""

from collections.abc import Generator
from datetime import date, datetime, timedelta, timezone
from decimal import Decimal
from enum import Enum, IntEnum
from os import environ
from typing import Literal
from uuid import UUID

import boto3
from moto import mock_aws
from mypy_boto3_dynamodb.service_resource import Table
from pydantic import BaseModel, Field
from pytest import fixture

from pydamodb.models import PrimaryKeyAndSortKeyModel, PrimaryKeyModel

# =============================================================================
# Enums for testing
# =============================================================================


class Status(str, Enum):
    """String enum for order/task status."""

    PENDING = "pending"
    PROCESSING = "processing"
    COMPLETED = "completed"
    CANCELLED = "cancelled"


class Priority(IntEnum):
    """Integer enum for priority levels."""

    LOW = 1
    MEDIUM = 2
    HIGH = 3
    CRITICAL = 4


class PaymentMethod(str, Enum):
    """Payment method options."""

    CREDIT_CARD = "credit_card"
    DEBIT_CARD = "debit_card"
    PAYPAL = "paypal"
    BANK_TRANSFER = "bank_transfer"
    CRYPTO = "crypto"


# =============================================================================
# Nested Pydantic Models (not DynamoDB models, just for embedding)
# =============================================================================


class Address(BaseModel):
    """Nested address model."""

    street: str
    city: str
    state: str
    postal_code: str
    country: str = "USA"
    is_primary: bool = False


class GeoLocation(BaseModel):
    """Geographic coordinates."""

    latitude: Decimal
    longitude: Decimal
    altitude_meters: float | None = None


class ContactInfo(BaseModel):
    """Contact information with multiple channels."""

    email: str
    phone: str | None = None
    secondary_email: str | None = None
    preferred_contact: Literal["email", "phone", "sms"] = "email"


class MonetaryAmount(BaseModel):
    """Money representation with currency."""

    amount: Decimal
    currency: str = "USD"


class DateRange(BaseModel):
    """A date range with start and end."""

    start_date: date
    end_date: date | None = None


class Dimensions(BaseModel):
    """Physical dimensions for products."""

    length_cm: Decimal
    width_cm: Decimal
    height_cm: Decimal
    weight_kg: Decimal


class ProductVariant(BaseModel):
    """Product variant with options."""

    sku: str
    color: str | None = None
    size: str | None = None
    price_modifier: Decimal = Decimal("0.00")
    stock_count: int = 0
    is_available: bool = True


class OrderLineItem(BaseModel):
    """Single line item in an order."""

    product_id: str
    product_name: str
    quantity: int
    unit_price: Decimal
    discount_percent: Decimal = Decimal("0.00")
    notes: str | None = None


class PaymentDetails(BaseModel):
    """Payment information for an order."""

    method: PaymentMethod
    transaction_id: str | None = None
    amount: MonetaryAmount
    paid_at: datetime | None = None
    is_refunded: bool = False


class ShippingInfo(BaseModel):
    """Shipping details for an order."""

    address: Address
    carrier: str | None = None
    tracking_number: str | None = None
    estimated_delivery: date | None = None
    shipped_at: datetime | None = None
    delivered_at: datetime | None = None
    signature_required: bool = False


class AuditMetadata(BaseModel):
    """Audit trail metadata."""

    created_at: datetime
    created_by: str
    updated_at: datetime | None = None
    updated_by: str | None = None
    version: int = 1


# =============================================================================
# Complex DynamoDB Models - Primary Key Only
# =============================================================================


class ComplexUser(PrimaryKeyModel):
    """A complex user model with many field types.

    Tests:
    - String fields
    - Optional fields
    - Nested models (Address, ContactInfo)
    - Lists of nested models
    - Sets (tags)
    - Enums
    - Dates and times
    - UUIDs
    - Booleans
    - Integers
    - Decimals
    """

    # Primary key (determined by table schema)
    user_id: str

    # Basic fields
    username: str
    display_name: str | None = None

    # Contact and address (nested models)
    contact: ContactInfo
    primary_address: Address | None = None
    additional_addresses: list[Address] = Field(default_factory=list)

    # Profile data
    bio: str | None = None
    avatar_url: str | None = None
    date_of_birth: date | None = None
    registration_date: datetime

    # Preferences stored as nested model
    preferences: dict[str, bool] = Field(default_factory=dict)

    # Tags and categories (for testing sets/lists)
    tags: list[str] = Field(default_factory=list)
    interests: list[str] = Field(default_factory=list)

    # Account status
    is_active: bool = True
    is_verified: bool = False
    is_premium: bool = False

    # Numeric fields
    login_count: int = 0
    account_balance: Decimal = Decimal("0.00")
    loyalty_points: int = 0

    # Audit
    last_login_at: datetime | None = None
    created_at: datetime
    updated_at: datetime | None = None


class ComplexProduct(PrimaryKeyModel):
    """A complex product catalog model.

    Tests:
    - Nested list of variants
    - Decimal precision for prices
    - Physical dimensions
    - Category hierarchies
    - Rich text descriptions
    - Multiple image URLs
    """

    # Primary key (determined by table schema)
    product_id: str

    # Basic info
    name: str
    slug: str
    description: str | None = None
    short_description: str | None = None

    # Categorization
    category: str
    subcategory: str | None = None
    brand: str | None = None
    manufacturer: str | None = None

    # Pricing
    base_price: Decimal
    sale_price: Decimal | None = None
    cost_price: Decimal | None = None
    currency: str = "USD"

    # Inventory
    stock_quantity: int = 0
    reserved_quantity: int = 0
    reorder_level: int = 10
    is_in_stock: bool = True
    is_backorderable: bool = False

    # Physical attributes
    dimensions: Dimensions | None = None
    is_fragile: bool = False
    requires_shipping: bool = True

    # Variants
    variants: list[ProductVariant] = Field(default_factory=list)

    # Media
    primary_image_url: str | None = None
    image_urls: list[str] = Field(default_factory=list)

    # SEO and discovery
    tags: list[str] = Field(default_factory=list)
    search_keywords: list[str] = Field(default_factory=list)

    # Status
    status: Status = Status.PENDING
    is_published: bool = False
    is_featured: bool = False

    # Dates
    created_at: datetime
    updated_at: datetime | None = None
    published_at: datetime | None = None


class ComplexConfig(PrimaryKeyModel):
    """Application configuration model testing various primitives.

    Tests:
    - Deep nesting
    - Mixed type dictionaries
    - Various numeric types
    - Binary data (bytes)
    - Complex default values
    """

    # Primary key (determined by table schema)
    config_key: str

    # Environment
    environment: Literal["development", "staging", "production"]

    # Feature flags (bool dict)
    feature_flags: dict[str, bool] = Field(default_factory=dict)

    # Rate limits (int dict)
    rate_limits: dict[str, int] = Field(default_factory=dict)

    # Thresholds (Decimal dict)
    thresholds: dict[str, Decimal] = Field(default_factory=dict)

    # Connection strings (sensitive, but testing storage)
    connection_strings: dict[str, str] = Field(default_factory=dict)

    # Nested settings
    email_settings: dict[str, str | int | bool] = Field(default_factory=dict)

    # Binary data (for testing bytes serialization)
    encryption_key: bytes | None = None
    certificate_data: bytes | None = None

    # Timeouts as timedelta (serialized to seconds)
    default_timeout_seconds: int = 30
    max_retry_attempts: int = 3

    # Version tracking
    schema_version: int = 1
    last_modified: datetime


# =============================================================================
# Complex DynamoDB Models - Partition + Sort Key
# =============================================================================


class ComplexOrder(PrimaryKeyAndSortKeyModel):
    """A complex e-commerce order model.

    Tests:
    - Composite key (customer_id + order_id)
    - Nested line items
    - Payment details
    - Shipping info
    - Status transitions
    - Monetary calculations
    """

    # Composite key (determined by table schema)
    customer_id: str
    order_id: str

    # Order details
    line_items: list[OrderLineItem] = Field(default_factory=list)
    item_count: int = 0

    # Pricing
    subtotal: Decimal = Decimal("0.00")
    tax_amount: Decimal = Decimal("0.00")
    shipping_cost: Decimal = Decimal("0.00")
    discount_amount: Decimal = Decimal("0.00")
    total: Decimal = Decimal("0.00")
    currency: str = "USD"

    # Payment
    payment: PaymentDetails | None = None
    is_paid: bool = False

    # Shipping
    shipping: ShippingInfo | None = None
    requires_signature: bool = False

    # Status tracking
    status: Status = Status.PENDING
    priority: Priority = Priority.MEDIUM

    # Notes and metadata
    customer_notes: str | None = None
    internal_notes: str | None = None
    tags: list[str] = Field(default_factory=list)

    # Timestamps
    created_at: datetime
    updated_at: datetime | None = None
    completed_at: datetime | None = None
    cancelled_at: datetime | None = None
    cancellation_reason: str | None = None


class ComplexEvent(PrimaryKeyAndSortKeyModel):
    """Event/audit log model for time-series data.

    Tests:
    - Time-based sort key (ISO format string)
    - Event payload as flexible dict
    - UUID fields
    - Geo location
    """

    # Composite key (determined by table schema)
    entity_id: str
    event_timestamp: str  # ISO format for lexicographic sorting

    # Event identification
    event_id: UUID
    event_type: str
    event_version: str = "1.0"

    # Event data - Note: DynamoDB doesn't support float, use Decimal for fractional numbers
    payload: dict[str, str | int | Decimal | bool | None] = Field(default_factory=dict)

    # Context
    source_system: str
    correlation_id: UUID | None = None
    causation_id: UUID | None = None

    # Actor info
    actor_id: str | None = None
    actor_type: Literal["user", "system", "external"] = "system"

    # Location (if applicable)
    location: GeoLocation | None = None

    # Processing status
    is_processed: bool = False
    processed_at: datetime | None = None
    processing_errors: list[str] = Field(default_factory=list)

    # TTL for auto-expiration (DynamoDB TTL feature)
    expires_at: int | None = None  # Unix timestamp


class ComplexSession(PrimaryKeyAndSortKeyModel):
    """User session model with device info.

    Tests:
    - User + session composite key
    - Device fingerprinting
    - Nested location data
    - Token storage
    """

    # Composite key (determined by table schema)
    user_id: str
    session_id: str

    # Session data
    access_token: str
    refresh_token: str | None = None
    token_expires_at: datetime

    # Device info
    device_id: str | None = None
    device_type: Literal["mobile", "tablet", "desktop", "unknown"] = "unknown"
    device_name: str | None = None
    os_name: str | None = None
    os_version: str | None = None
    browser_name: str | None = None
    browser_version: str | None = None

    # Network info
    ip_address: str | None = None
    user_agent: str | None = None
    location: GeoLocation | None = None

    # Session state
    is_active: bool = True
    is_remembered: bool = False
    last_activity_at: datetime

    # Security
    mfa_verified: bool = False
    security_level: int = 1

    # Timestamps
    created_at: datetime
    expires_at: datetime


class ComplexTask(PrimaryKeyAndSortKeyModel):
    """Project task/todo model with rich metadata.

    Tests:
    - Hierarchical data (parent tasks)
    - Date/time handling
    - Recursive references via IDs
    - Assignee lists
    """

    # Composite key (determined by table schema)
    project_id: str
    task_id: str

    # Basic info
    title: str
    description: str | None = None
    status: Status = Status.PENDING
    priority: Priority = Priority.MEDIUM

    # Hierarchy
    parent_task_id: str | None = None
    subtask_ids: list[str] = Field(default_factory=list)

    # Assignment
    assignee_ids: list[str] = Field(default_factory=list)
    reporter_id: str
    watchers: list[str] = Field(default_factory=list)

    # Time tracking
    estimated_hours: Decimal | None = None
    logged_hours: Decimal = Decimal("0.00")
    due_date: date | None = None
    start_date: date | None = None
    completed_date: date | None = None

    # Scheduling
    scheduled_start: datetime | None = None
    scheduled_end: datetime | None = None
    reminder_at: datetime | None = None

    # Categorization
    labels: list[str] = Field(default_factory=list)
    sprint_id: str | None = None
    milestone_id: str | None = None

    # Links and attachments (stored as URLs/IDs)
    attachment_ids: list[str] = Field(default_factory=list)
    related_task_ids: list[str] = Field(default_factory=list)
    external_links: list[str] = Field(default_factory=list)

    # Comments count (denormalized for quick access)
    comment_count: int = 0

    # Audit
    audit: AuditMetadata


# =============================================================================
# AWS Credentials Fixture (session-scoped)
# =============================================================================


@fixture(scope="session", autouse=True)
def aws_credentials() -> None:
    """Set up fake AWS credentials for moto."""
    environ["AWS_ACCESS_KEY_ID"] = "testing"
    environ["AWS_SECRET_ACCESS_KEY"] = "testing"  # noqa: S105
    environ["AWS_SECURITY_TOKEN"] = "testing"  # noqa: S105
    environ["AWS_SESSION_TOKEN"] = "testing"  # noqa: S105
    environ["AWS_DEFAULT_REGION"] = "us-east-1"


# =============================================================================
# DynamoDB Table Fixtures
# =============================================================================


@fixture
def pk_table() -> Generator[Table, None, None]:
    """Create a DynamoDB table with only a partition key."""
    with mock_aws():
        dynamodb = boto3.resource("dynamodb")
        table = dynamodb.create_table(
            TableName="PKTable",
            KeySchema=[
                {"AttributeName": "id", "KeyType": "HASH"},
            ],
            AttributeDefinitions=[
                {"AttributeName": "id", "AttributeType": "S"},
            ],
            BillingMode="PAY_PER_REQUEST",
        )
        table.wait_until_exists()
        yield table
        table.delete()


@fixture
def pk_sk_table() -> Generator[Table, None, None]:
    """Create a DynamoDB table with partition and sort key."""
    with mock_aws():
        dynamodb = boto3.resource("dynamodb")
        table = dynamodb.create_table(
            TableName="PKSKTable",
            KeySchema=[
                {"AttributeName": "id", "KeyType": "HASH"},
                {"AttributeName": "sort", "KeyType": "RANGE"},
            ],
            AttributeDefinitions=[
                {"AttributeName": "id", "AttributeType": "S"},
                {"AttributeName": "sort", "AttributeType": "S"},
            ],
            BillingMode="PAY_PER_REQUEST",
        )
        table.wait_until_exists()
        yield table
        table.delete()


@fixture
def user_table() -> Generator[Table, None, None]:
    """Create a table for ComplexUser models."""
    with mock_aws():
        dynamodb = boto3.resource("dynamodb")
        table = dynamodb.create_table(
            TableName="Users",
            KeySchema=[
                {"AttributeName": "user_id", "KeyType": "HASH"},
            ],
            AttributeDefinitions=[
                {"AttributeName": "user_id", "AttributeType": "S"},
            ],
            BillingMode="PAY_PER_REQUEST",
        )
        table.wait_until_exists()
        yield table
        table.delete()


@fixture
def product_table() -> Generator[Table, None, None]:
    """Create a table for ComplexProduct models."""
    with mock_aws():
        dynamodb = boto3.resource("dynamodb")
        table = dynamodb.create_table(
            TableName="Products",
            KeySchema=[
                {"AttributeName": "product_id", "KeyType": "HASH"},
            ],
            AttributeDefinitions=[
                {"AttributeName": "product_id", "AttributeType": "S"},
            ],
            BillingMode="PAY_PER_REQUEST",
        )
        table.wait_until_exists()
        yield table
        table.delete()


@fixture
def order_table() -> Generator[Table, None, None]:
    """Create a table for ComplexOrder models."""
    with mock_aws():
        dynamodb = boto3.resource("dynamodb")
        table = dynamodb.create_table(
            TableName="Orders",
            KeySchema=[
                {"AttributeName": "customer_id", "KeyType": "HASH"},
                {"AttributeName": "order_id", "KeyType": "RANGE"},
            ],
            AttributeDefinitions=[
                {"AttributeName": "customer_id", "AttributeType": "S"},
                {"AttributeName": "order_id", "AttributeType": "S"},
            ],
            BillingMode="PAY_PER_REQUEST",
        )
        table.wait_until_exists()
        yield table
        table.delete()


@fixture
def event_table() -> Generator[Table, None, None]:
    """Create a table for ComplexEvent models."""
    with mock_aws():
        dynamodb = boto3.resource("dynamodb")
        table = dynamodb.create_table(
            TableName="Events",
            KeySchema=[
                {"AttributeName": "entity_id", "KeyType": "HASH"},
                {"AttributeName": "event_timestamp", "KeyType": "RANGE"},
            ],
            AttributeDefinitions=[
                {"AttributeName": "entity_id", "AttributeType": "S"},
                {"AttributeName": "event_timestamp", "AttributeType": "S"},
            ],
            BillingMode="PAY_PER_REQUEST",
        )
        table.wait_until_exists()
        yield table
        table.delete()


@fixture
def task_table() -> Generator[Table, None, None]:
    """Create a table for ComplexTask models."""
    with mock_aws():
        dynamodb = boto3.resource("dynamodb")
        table = dynamodb.create_table(
            TableName="Tasks",
            KeySchema=[
                {"AttributeName": "project_id", "KeyType": "HASH"},
                {"AttributeName": "task_id", "KeyType": "RANGE"},
            ],
            AttributeDefinitions=[
                {"AttributeName": "project_id", "AttributeType": "S"},
                {"AttributeName": "task_id", "AttributeType": "S"},
            ],
            BillingMode="PAY_PER_REQUEST",
        )
        table.wait_until_exists()
        yield table
        table.delete()


# =============================================================================
# Sample Data Factory Functions
# =============================================================================


def create_sample_address(
    *,
    street: str = "123 Main St",
    city: str = "Springfield",
    state: str = "IL",
    postal_code: str = "62701",
    country: str = "USA",
    is_primary: bool = False,
) -> Address:
    """Create a sample Address for testing."""
    return Address(
        street=street,
        city=city,
        state=state,
        postal_code=postal_code,
        country=country,
        is_primary=is_primary,
    )


def create_sample_contact(
    *,
    email: str = "test@example.com",
    phone: str | None = "+1-555-123-4567",
    preferred_contact: Literal["email", "phone", "sms"] = "email",
) -> ContactInfo:
    """Create a sample ContactInfo for testing."""
    return ContactInfo(
        email=email,
        phone=phone,
        preferred_contact=preferred_contact,
    )


def create_sample_user(
    user_id: str = "user-001",
    *,
    username: str = "johndoe",
    email: str = "john@example.com",
    with_addresses: bool = True,
) -> ComplexUser:
    """Create a sample ComplexUser for testing."""
    now = datetime.now(timezone.utc)

    user = ComplexUser(
        user_id=user_id,
        username=username,
        display_name="John Doe",
        contact=create_sample_contact(email=email),
        registration_date=now,
        created_at=now,
        tags=["beta-tester", "premium"],
        interests=["technology", "music", "travel"],
        is_active=True,
        is_verified=True,
        account_balance=Decimal("150.50"),
        loyalty_points=1250,
    )

    if with_addresses:
        user.primary_address = create_sample_address(is_primary=True)
        user.additional_addresses = [
            create_sample_address(
                street="456 Oak Ave",
                city="Chicago",
                state="IL",
                postal_code="60601",
            ),
        ]

    return user


def create_sample_product(
    product_id: str = "prod-001",
    *,
    name: str = "Wireless Headphones",
    price: Decimal = Decimal("99.99"),
    with_variants: bool = True,
) -> ComplexProduct:
    """Create a sample ComplexProduct for testing."""
    now = datetime.now(timezone.utc)

    product = ComplexProduct(
        product_id=product_id,
        name=name,
        slug=name.lower().replace(" ", "-"),
        description="High-quality wireless headphones with noise cancellation.",
        category="Electronics",
        subcategory="Audio",
        brand="TechBrand",
        base_price=price,
        sale_price=price * Decimal("0.8"),  # 20% off
        stock_quantity=50,
        dimensions=Dimensions(
            length_cm=Decimal("20.0"),
            width_cm=Decimal("18.0"),
            height_cm=Decimal("8.0"),
            weight_kg=Decimal("0.3"),
        ),
        tags=["wireless", "bluetooth", "noise-cancelling"],
        search_keywords=["headphones", "audio", "music", "wireless"],
        status=Status.COMPLETED,
        is_published=True,
        created_at=now,
    )

    if with_variants:
        product.variants = [
            ProductVariant(
                sku=f"{product_id}-BLK",
                color="Black",
                stock_count=30,
                is_available=True,
            ),
            ProductVariant(
                sku=f"{product_id}-WHT",
                color="White",
                stock_count=15,
                is_available=True,
            ),
            ProductVariant(
                sku=f"{product_id}-RED",
                color="Red",
                price_modifier=Decimal("10.00"),
                stock_count=5,
                is_available=True,
            ),
        ]

    return product


def create_sample_order(
    customer_id: str = "cust-001",
    order_id: str = "ord-001",
    *,
    with_payment: bool = True,
    with_shipping: bool = True,
) -> ComplexOrder:
    """Create a sample ComplexOrder for testing."""
    now = datetime.now(timezone.utc)

    line_items = [
        OrderLineItem(
            product_id="prod-001",
            product_name="Wireless Headphones",
            quantity=1,
            unit_price=Decimal("99.99"),
        ),
        OrderLineItem(
            product_id="prod-002",
            product_name="Phone Case",
            quantity=2,
            unit_price=Decimal("19.99"),
            discount_percent=Decimal("10.00"),
        ),
    ]

    subtotal = sum(
        item.unit_price * item.quantity * (1 - item.discount_percent / 100)
        for item in line_items
    )
    tax = subtotal * Decimal("0.08")
    shipping = Decimal("5.99")
    total = subtotal + tax + shipping

    order = ComplexOrder(
        customer_id=customer_id,
        order_id=order_id,
        line_items=line_items,
        item_count=sum(item.quantity for item in line_items),
        subtotal=subtotal,
        tax_amount=tax,
        shipping_cost=shipping,
        total=total,
        status=Status.PROCESSING,
        priority=Priority.MEDIUM,
        created_at=now,
    )

    if with_payment:
        order.payment = PaymentDetails(
            method=PaymentMethod.CREDIT_CARD,
            transaction_id="txn-12345",
            amount=MonetaryAmount(amount=total),
            paid_at=now,
        )
        order.is_paid = True

    if with_shipping:
        order.shipping = ShippingInfo(
            address=create_sample_address(),
            carrier="UPS",
            estimated_delivery=date.today() + timedelta(days=5),
        )

    return order


def create_sample_event(
    entity_id: str = "entity-001",
    event_type: str = "user.created",
    *,
    event_id: UUID | None = None,
) -> ComplexEvent:
    """Create a sample ComplexEvent for testing."""
    from uuid import uuid4

    now = datetime.now(timezone.utc)

    return ComplexEvent(
        entity_id=entity_id,
        event_timestamp=now.isoformat(),
        event_id=event_id or uuid4(),
        event_type=event_type,
        payload={
            "user_id": "user-001",
            "action": "registration",
            "ip_address": "192.168.1.1",
            "success": True,
        },
        source_system="auth-service",
        actor_id="system",
        actor_type="system",
        location=GeoLocation(
            latitude=Decimal("41.8781"),
            longitude=Decimal("-87.6298"),
        ),
    )


def create_sample_task(
    project_id: str = "proj-001",
    task_id: str = "task-001",
    *,
    title: str = "Implement feature X",
    reporter_id: str = "user-001",
) -> ComplexTask:
    """Create a sample ComplexTask for testing."""
    now = datetime.now(timezone.utc)

    return ComplexTask(
        project_id=project_id,
        task_id=task_id,
        title=title,
        description="Detailed description of the task requirements.",
        status=Status.PENDING,
        priority=Priority.HIGH,
        assignee_ids=["user-002", "user-003"],
        reporter_id=reporter_id,
        watchers=["user-001", "user-004"],
        estimated_hours=Decimal("8.0"),
        due_date=date.today() + timedelta(days=7),
        labels=["feature", "backend", "high-priority"],
        sprint_id="sprint-2024-01",
        audit=AuditMetadata(
            created_at=now,
            created_by=reporter_id,
        ),
    )
