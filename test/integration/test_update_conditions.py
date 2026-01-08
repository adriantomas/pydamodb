"""Integration tests for update operations with condition expressions."""

import pytest
from mypy_boto3_dynamodb.service_resource import Table

from pydamodb.conditions import AttributeNotExists
from pydamodb.exceptions import ConditionCheckFailedError
from pydamodb.models import PrimaryKeyAndSortKeyModel, PrimaryKeyModel, PydamoConfig

# =============================================================================
# Test Models
# =============================================================================


class User(PrimaryKeyModel):
    """Test model for PK-only table."""

    id: str
    name: str
    age: int
    status: str
    email: str | None = None
    tags: list[str] | None = None


class Order(PrimaryKeyAndSortKeyModel):
    """Test model for PK+SK table."""

    id: str
    sort: str
    product: str
    quantity: int
    status: str


# =============================================================================
# Fixtures
# =============================================================================


@pytest.fixture
def user_model(pk_table: Table) -> User:
    """Create a User model configured with the PK table."""
    User.pydamo_config = PydamoConfig(table=pk_table)
    user = User(
        id="user-1", name="John Doe", age=30, status="active", email="john@example.com"
    )
    user.save()
    return user


@pytest.fixture
def order_model(pk_sk_table: Table) -> Order:
    """Create an Order model configured with the PK+SK table."""
    Order.pydamo_config = PydamoConfig(table=pk_sk_table)
    order = Order(
        id="user-1",
        sort="order-1",
        product="Widget",
        quantity=5,
        status="pending",
    )
    order.save()
    return order


# =============================================================================
# Basic Update Tests (No Condition)
# =============================================================================


class TestBasicUpdate:
    """Tests for update operations without conditions."""

    def test_update_single_field(self, user_model: User) -> None:
        """Update a single field."""
        User.update_item("user-1", updates={User.attr.name: "Jane Doe"})

        updated = User.get_item("user-1")
        assert updated is not None
        assert updated.name == "Jane Doe"
        assert updated.age == 30  # unchanged

    def test_update_multiple_fields(self, user_model: User) -> None:
        """Update multiple fields at once."""
        User.update_item(
            "user-1",
            updates={
                User.attr.name: "Jane Doe",
                User.attr.age: 25,
                User.attr.status: "inactive",
            },
        )

        updated = User.get_item("user-1")
        assert updated is not None
        assert updated.name == "Jane Doe"
        assert updated.age == 25
        assert updated.status == "inactive"

    def test_update_pk_sk_model(self, order_model: Order) -> None:
        """Update a PK+SK model."""
        Order.update_item(
            "user-1",
            "order-1",
            updates={Order.attr.quantity: 10, Order.attr.status: "shipped"},
        )

        updated = Order.get_item("user-1", "order-1")
        assert updated is not None
        assert updated.quantity == 10
        assert updated.status == "shipped"


# =============================================================================
# Comparison Condition Tests (==, !=, <, <=, >, >=)
# =============================================================================


class TestComparisonConditions:
    """Tests for comparison condition operators."""

    def test_eq_condition_success(self, user_model: User) -> None:
        """Update succeeds when == condition is met."""
        User.update_item(
            "user-1",
            updates={User.attr.name: "Updated Name"},
            condition=User.attr.status == "active",
        )

        updated = User.get_item("user-1")
        assert updated is not None
        assert updated.name == "Updated Name"

    def test_eq_condition_failure(self, user_model: User) -> None:
        """Update fails when == condition is not met."""
        with pytest.raises(ConditionCheckFailedError):
            User.update_item(
                "user-1",
                updates={User.attr.name: "Updated Name"},
                condition=User.attr.status == "inactive",
            )

    def test_ne_condition_success(self, user_model: User) -> None:
        """Update succeeds when != condition is met."""
        User.update_item(
            "user-1",
            updates={User.attr.name: "Updated Name"},
            condition=User.attr.status != "inactive",
        )

        updated = User.get_item("user-1")
        assert updated is not None
        assert updated.name == "Updated Name"

    def test_ne_condition_failure(self, user_model: User) -> None:
        """Update fails when != condition is not met."""
        with pytest.raises(ConditionCheckFailedError):
            User.update_item(
                "user-1",
                updates={User.attr.name: "Updated Name"},
                condition=User.attr.status != "active",
            )

    def test_lt_condition_success(self, user_model: User) -> None:
        """Update succeeds when < condition is met."""
        User.update_item(
            "user-1",
            updates={User.attr.name: "Updated Name"},
            condition=User.attr.age < 40,
        )

        updated = User.get_item("user-1")
        assert updated is not None
        assert updated.name == "Updated Name"

    def test_lt_condition_failure(self, user_model: User) -> None:
        """Update fails when < condition is not met."""
        with pytest.raises(ConditionCheckFailedError):
            User.update_item(
                "user-1",
                updates={User.attr.name: "Updated Name"},
                condition=User.attr.age < 30,
            )

    def test_lte_condition_success(self, user_model: User) -> None:
        """Update succeeds when <= condition is met."""
        User.update_item(
            "user-1",
            updates={User.attr.name: "Updated Name"},
            condition=User.attr.age <= 30,
        )

        updated = User.get_item("user-1")
        assert updated is not None
        assert updated.name == "Updated Name"

    def test_lte_condition_failure(self, user_model: User) -> None:
        """Update fails when <= condition is not met."""
        with pytest.raises(ConditionCheckFailedError):
            User.update_item(
                "user-1",
                updates={User.attr.name: "Updated Name"},
                condition=User.attr.age <= 29,
            )

    def test_gt_condition_success(self, user_model: User) -> None:
        """Update succeeds when > condition is met."""
        User.update_item(
            "user-1",
            updates={User.attr.name: "Updated Name"},
            condition=User.attr.age > 20,
        )

        updated = User.get_item("user-1")
        assert updated is not None
        assert updated.name == "Updated Name"

    def test_gt_condition_failure(self, user_model: User) -> None:
        """Update fails when > condition is not met."""
        with pytest.raises(ConditionCheckFailedError):
            User.update_item(
                "user-1",
                updates={User.attr.name: "Updated Name"},
                condition=User.attr.age > 30,
            )

    def test_gte_condition_success(self, user_model: User) -> None:
        """Update succeeds when >= condition is met."""
        User.update_item(
            "user-1",
            updates={User.attr.name: "Updated Name"},
            condition=User.attr.age >= 30,
        )

        updated = User.get_item("user-1")
        assert updated is not None
        assert updated.name == "Updated Name"

    def test_gte_condition_failure(self, user_model: User) -> None:
        """Update fails when >= condition is not met."""
        with pytest.raises(ConditionCheckFailedError):
            User.update_item(
                "user-1",
                updates={User.attr.name: "Updated Name"},
                condition=User.attr.age >= 31,
            )


# =============================================================================
# Function Condition Tests (between, begins_with, contains, exists, not_exists)
# =============================================================================


class TestFunctionConditions:
    """Tests for function-based conditions."""

    def test_between_condition_success(self, user_model: User) -> None:
        """Update succeeds when BETWEEN condition is met."""
        User.update_item(
            "user-1",
            updates={User.attr.name: "Updated Name"},
            condition=User.attr.age.between(25, 35),
        )

        updated = User.get_item("user-1")
        assert updated is not None
        assert updated.name == "Updated Name"

    def test_between_condition_failure(self, user_model: User) -> None:
        """Update fails when BETWEEN condition is not met."""
        with pytest.raises(ConditionCheckFailedError):
            User.update_item(
                "user-1",
                updates={User.attr.name: "Updated Name"},
                condition=User.attr.age.between(35, 45),
            )

    def test_begins_with_condition_success(self, user_model: User) -> None:
        """Update succeeds when begins_with condition is met."""
        User.update_item(
            "user-1",
            updates={User.attr.age: 31},
            condition=User.attr.name.begins_with("John"),
        )

        updated = User.get_item("user-1")
        assert updated is not None
        assert updated.age == 31

    def test_begins_with_condition_failure(self, user_model: User) -> None:
        """Update fails when begins_with condition is not met."""
        with pytest.raises(ConditionCheckFailedError):
            User.update_item(
                "user-1",
                updates={User.attr.age: 31},
                condition=User.attr.name.begins_with("Jane"),
            )

    def test_contains_condition_success(self, user_model: User) -> None:
        """Update succeeds when contains condition is met."""
        User.update_item(
            "user-1",
            updates={User.attr.age: 31},
            condition=User.attr.email.contains("@example"),
        )

        updated = User.get_item("user-1")
        assert updated is not None
        assert updated.age == 31

    def test_contains_condition_failure(self, user_model: User) -> None:
        """Update fails when contains condition is not met."""
        with pytest.raises(ConditionCheckFailedError):
            User.update_item(
                "user-1",
                updates={User.attr.age: 31},
                condition=User.attr.email.contains("@gmail"),
            )

    def test_attribute_exists_condition_success(self, user_model: User) -> None:
        """Update succeeds when attribute_exists condition is met."""
        User.update_item(
            "user-1",
            updates={User.attr.name: "Updated Name"},
            condition=User.attr.email.exists(),
        )

        updated = User.get_item("user-1")
        assert updated is not None
        assert updated.name == "Updated Name"

    def test_attribute_exists_condition_failure(self, user_model: User) -> None:
        """Update fails when attribute_exists condition is not met (item doesn't exist)."""
        with pytest.raises(ConditionCheckFailedError):
            User.update_item(
                "non-existent-user",
                updates={User.attr.name: "Updated Name"},
                condition=User.attr.email.exists(),
            )

    def test_attribute_not_exists_condition_success(self, user_model: User) -> None:
        """Update succeeds when attribute_not_exists condition is met.

        We test with an attribute that truly doesn't exist in the stored item
        by using AttributeNotExists directly with a non-existent field name.
        """
        # Use AttributeNotExists with a field that doesn't exist in the model
        User.update_item(
            "user-1",
            updates={User.attr.name: "Updated Name"},
            condition=AttributeNotExists("nonexistent_field"),
        )

        updated = User.get_item("user-1")
        assert updated is not None
        assert updated.name == "Updated Name"

    def test_attribute_not_exists_condition_failure(self, user_model: User) -> None:
        """Update fails when attribute_not_exists condition is not met."""
        with pytest.raises(ConditionCheckFailedError):
            User.update_item(
                "user-1",
                updates={User.attr.name: "Updated Name"},
                condition=User.attr.email.not_exists(),
            )

    def test_in_condition_success(self, user_model: User) -> None:
        """Update succeeds when IN condition is met."""
        User.update_item(
            "user-1",
            updates={User.attr.name: "Updated Name"},
            condition=User.attr.status.in_("active", "pending", "inactive"),
        )

        updated = User.get_item("user-1")
        assert updated is not None
        assert updated.name == "Updated Name"

    def test_in_condition_failure(self, user_model: User) -> None:
        """Update fails when IN condition is not met."""
        with pytest.raises(ConditionCheckFailedError):
            User.update_item(
                "user-1",
                updates={User.attr.name: "Updated Name"},
                condition=User.attr.status.in_("pending", "inactive", "archived"),
            )

    def test_in_condition_with_numbers(self, user_model: User) -> None:
        """Update succeeds when IN condition with numbers is met."""
        User.update_item(
            "user-1",
            updates={User.attr.name: "Updated Name"},
            condition=User.attr.age.in_(25, 30, 35),
        )

        updated = User.get_item("user-1")
        assert updated is not None
        assert updated.name == "Updated Name"

    def test_size_gt_condition_success(self, user_model: User) -> None:
        """Update succeeds when size > condition is met."""
        User.update_item(
            "user-1",
            updates={User.attr.age: 31},
            condition=User.attr.name.size() > 0,
        )

        updated = User.get_item("user-1")
        assert updated is not None
        assert updated.age == 31

    def test_size_gt_condition_failure(self, user_model: User) -> None:
        """Update fails when size > condition is not met."""
        with pytest.raises(ConditionCheckFailedError):
            User.update_item(
                "user-1",
                updates={User.attr.age: 31},
                # "John Doe" is 8 characters, so size > 10 should fail
                condition=User.attr.name.size() > 10,
            )

    def test_size_gte_condition_success(self, user_model: User) -> None:
        """Update succeeds when size >= condition is met."""
        # "John Doe" is 8 characters
        User.update_item(
            "user-1",
            updates={User.attr.age: 31},
            condition=User.attr.name.size() >= 8,
        )

        updated = User.get_item("user-1")
        assert updated is not None
        assert updated.age == 31

    def test_size_lt_condition_success(self, user_model: User) -> None:
        """Update succeeds when size < condition is met."""
        User.update_item(
            "user-1",
            updates={User.attr.age: 31},
            condition=User.attr.name.size() < 20,
        )

        updated = User.get_item("user-1")
        assert updated is not None
        assert updated.age == 31

    def test_size_eq_condition_success(self, user_model: User) -> None:
        """Update succeeds when size == condition is met."""
        # "John Doe" is 8 characters
        User.update_item(
            "user-1",
            updates={User.attr.age: 31},
            condition=User.attr.name.size() == 8,
        )

        updated = User.get_item("user-1")
        assert updated is not None
        assert updated.age == 31

    def test_size_ne_condition_success(self, user_model: User) -> None:
        """Update succeeds when size != condition is met."""
        User.update_item(
            "user-1",
            updates={User.attr.age: 31},
            condition=User.attr.name.size() != 100,
        )

        updated = User.get_item("user-1")
        assert updated is not None
        assert updated.age == 31


# =============================================================================
# Logical Operator Tests (AND, OR, NOT)
# =============================================================================


class TestLogicalOperators:
    """Tests for logical operators combining conditions."""

    def test_and_operator_success(self, user_model: User) -> None:
        """Update succeeds when AND condition is met."""
        User.update_item(
            "user-1",
            updates={User.attr.name: "Updated Name"},
            condition=(User.attr.status == "active") & (User.attr.age >= 18),
        )

        updated = User.get_item("user-1")
        assert updated is not None
        assert updated.name == "Updated Name"

    def test_and_operator_partial_failure(self, user_model: User) -> None:
        """Update fails when only one part of AND condition is met."""
        with pytest.raises(ConditionCheckFailedError):
            User.update_item(
                "user-1",
                updates={User.attr.name: "Updated Name"},
                condition=(User.attr.status == "active") & (User.attr.age >= 40),
            )

    def test_or_operator_first_true(self, user_model: User) -> None:
        """Update succeeds when first OR condition is met."""
        User.update_item(
            "user-1",
            updates={User.attr.name: "Updated Name"},
            condition=(User.attr.status == "active") | (User.attr.age >= 40),
        )

        updated = User.get_item("user-1")
        assert updated is not None
        assert updated.name == "Updated Name"

    def test_or_operator_second_true(self, user_model: User) -> None:
        """Update succeeds when second OR condition is met."""
        User.update_item(
            "user-1",
            updates={User.attr.name: "Updated Name"},
            condition=(User.attr.status == "inactive") | (User.attr.age == 30),
        )

        updated = User.get_item("user-1")
        assert updated is not None
        assert updated.name == "Updated Name"

    def test_or_operator_failure(self, user_model: User) -> None:
        """Update fails when neither OR condition is met."""
        with pytest.raises(ConditionCheckFailedError):
            User.update_item(
                "user-1",
                updates={User.attr.name: "Updated Name"},
                condition=(User.attr.status == "inactive") | (User.attr.age >= 40),
            )

    def test_not_operator_success(self, user_model: User) -> None:
        """Update succeeds when NOT condition is met."""
        User.update_item(
            "user-1",
            updates={User.attr.name: "Updated Name"},
            condition=~(User.attr.status == "inactive"),
        )

        updated = User.get_item("user-1")
        assert updated is not None
        assert updated.name == "Updated Name"

    def test_not_operator_failure(self, user_model: User) -> None:
        """Update fails when NOT condition is not met."""
        with pytest.raises(ConditionCheckFailedError):
            User.update_item(
                "user-1",
                updates={User.attr.name: "Updated Name"},
                condition=~(User.attr.status == "active"),
            )

    def test_complex_nested_conditions(self, user_model: User) -> None:
        """Update with complex nested conditions."""
        # (active AND age >= 18) OR (name starts with "John")
        condition = (
            (User.attr.status == "active") & (User.attr.age >= 18)
        ) | User.attr.name.begins_with("John")

        User.update_item(
            "user-1",
            updates={User.attr.name: "Updated Name"},
            condition=condition,
        )

        updated = User.get_item("user-1")
        assert updated is not None
        assert updated.name == "Updated Name"

    def test_chained_and_operators(self, user_model: User) -> None:
        """Update with multiple chained AND operators."""
        condition = (
            (User.attr.status == "active") & (User.attr.age >= 18) & (User.attr.email.exists())
        )

        User.update_item(
            "user-1",
            updates={User.attr.name: "Updated Name"},
            condition=condition,
        )

        updated = User.get_item("user-1")
        assert updated is not None
        assert updated.name == "Updated Name"

    def test_chained_or_operators(self, user_model: User) -> None:
        """Update with multiple chained OR operators."""
        condition = (User.attr.age == 30) | (User.attr.age == 25) | (User.attr.age == 35)

        User.update_item(
            "user-1",
            updates={User.attr.name: "Updated Name"},
            condition=condition,
        )

        updated = User.get_item("user-1")
        assert updated is not None
        assert updated.name == "Updated Name"


# =============================================================================
# PK+SK Model Condition Tests
# =============================================================================


class TestPKSKModelConditions:
    """Tests for conditions on PK+SK models."""

    def test_update_with_eq_condition(self, order_model: Order) -> None:
        """Update PK+SK model with == condition."""
        Order.update_item(
            "user-1",
            "order-1",
            updates={Order.attr.status: "shipped"},
            condition=Order.attr.status == "pending",
        )

        updated = Order.get_item("user-1", "order-1")
        assert updated is not None
        assert updated.status == "shipped"

    def test_update_with_numeric_condition(self, order_model: Order) -> None:
        """Update PK+SK model with numeric comparison."""
        Order.update_item(
            "user-1",
            "order-1",
            updates={Order.attr.quantity: 10},
            condition=Order.attr.quantity < 10,
        )

        updated = Order.get_item("user-1", "order-1")
        assert updated is not None
        assert updated.quantity == 10

    def test_update_with_combined_conditions(self, order_model: Order) -> None:
        """Update PK+SK model with combined conditions."""
        Order.update_item(
            "user-1",
            "order-1",
            updates={Order.attr.status: "confirmed"},
            condition=(Order.attr.status == "pending") & (Order.attr.quantity > 0),
        )

        updated = Order.get_item("user-1", "order-1")
        assert updated is not None
        assert updated.status == "confirmed"

    def test_update_fails_with_wrong_condition(self, order_model: Order) -> None:
        """Update PK+SK model fails with wrong condition."""
        with pytest.raises(ConditionCheckFailedError):
            Order.update_item(
                "user-1",
                "order-1",
                updates={Order.attr.status: "shipped"},
                condition=Order.attr.status == "shipped",
            )
