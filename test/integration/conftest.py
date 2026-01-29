from collections.abc import AsyncGenerator, Generator
from os import environ

import aioboto3
import boto3
from mypy_boto3_dynamodb.service_resource import DynamoDBServiceResource, Table
from pytest import fixture
from pytest_asyncio import fixture as async_fixture
from testcontainers.core.container import DockerContainer  # type: ignore[import-untyped]
from testcontainers.core.wait_strategies import (  # type: ignore[import-untyped]
    HttpWaitStrategy,
)
from types_aiobotocore_dynamodb.service_resource import (
    DynamoDBServiceResource as AsyncDynamoDBServiceResource,
    Table as AsyncTable,
)


@fixture(scope="session", autouse=True)
def aws_credentials() -> None:
    environ["AWS_ACCESS_KEY_ID"] = "testing"
    environ["AWS_SECRET_ACCESS_KEY"] = "testing"  # noqa: S105
    environ["AWS_SECURITY_TOKEN"] = "testing"  # noqa: S105
    environ["AWS_SESSION_TOKEN"] = "testing"  # noqa: S105
    environ["AWS_DEFAULT_REGION"] = "us-east-1"


@fixture(scope="session")
def dynamodb(aws_credentials: None) -> Generator[DynamoDBServiceResource, None, None]:
    with DockerContainer(
        "amazon/dynamodb-local:latest",
        ports=[8000],
        _wait_strategy=HttpWaitStrategy(8000).for_status_code(400),
    ) as container:
        dynamodb = boto3.resource(
            "dynamodb", endpoint_url=f"http://localhost:{container.get_exposed_port(8000)}"
        )
        yield dynamodb


@fixture
def pk_table(dynamodb: DynamoDBServiceResource) -> Generator[Table, None, None]:
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
def pk_sk_table(dynamodb: DynamoDBServiceResource) -> Generator[Table, None, None]:
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
def pk_sk_table_with_gsi(dynamodb: DynamoDBServiceResource) -> Generator[Table, None, None]:
    """Table with PK+SK and a GSI on 'status' attribute."""
    table = dynamodb.create_table(
        TableName="PKSKTableWithGSI",
        KeySchema=[
            {"AttributeName": "id", "KeyType": "HASH"},
            {"AttributeName": "sort", "KeyType": "RANGE"},
        ],
        AttributeDefinitions=[
            {"AttributeName": "id", "AttributeType": "S"},
            {"AttributeName": "sort", "AttributeType": "S"},
            {"AttributeName": "status", "AttributeType": "S"},
        ],
        GlobalSecondaryIndexes=[
            {
                "IndexName": "status-index",
                "KeySchema": [
                    {"AttributeName": "status", "KeyType": "HASH"},
                ],
                "Projection": {"ProjectionType": "ALL"},
            },
        ],
        BillingMode="PAY_PER_REQUEST",
    )
    table.wait_until_exists()

    yield table

    table.delete()


@fixture
def pk_sk_table_with_lsi(dynamodb: DynamoDBServiceResource) -> Generator[Table, None, None]:
    table = dynamodb.create_table(
        TableName="PKSKTableWithLSI",
        KeySchema=[
            {"AttributeName": "id", "KeyType": "HASH"},
            {"AttributeName": "sort", "KeyType": "RANGE"},
        ],
        AttributeDefinitions=[
            {"AttributeName": "id", "AttributeType": "S"},
            {"AttributeName": "sort", "AttributeType": "S"},
            {"AttributeName": "created_at", "AttributeType": "S"},
        ],
        LocalSecondaryIndexes=[
            {
                "IndexName": "created-at-index",
                "KeySchema": [
                    {"AttributeName": "id", "KeyType": "HASH"},
                    {"AttributeName": "created_at", "KeyType": "RANGE"},
                ],
                "Projection": {"ProjectionType": "ALL"},
            },
        ],
        BillingMode="PAY_PER_REQUEST",
    )
    table.wait_until_exists()

    yield table

    table.delete()


# Async fixtures


@fixture(scope="session")
def async_dynamodb_endpoint(aws_credentials: None) -> Generator[str, None, None]:
    """Session-scoped Docker container providing endpoint URL for async tests."""
    with DockerContainer(
        "amazon/dynamodb-local:latest",
        ports=[8000],
        _wait_strategy=HttpWaitStrategy(8000).for_status_code(400),
    ) as container:
        yield f"http://localhost:{container.get_exposed_port(8000)}"


@async_fixture
async def async_dynamodb(
    async_dynamodb_endpoint: str,
) -> AsyncGenerator[AsyncDynamoDBServiceResource, None]:
    """Function-scoped aioboto3 resource reusing session-scoped endpoint."""
    session = aioboto3.Session()
    async with session.resource("dynamodb", endpoint_url=async_dynamodb_endpoint) as dynamodb:
        yield dynamodb


@async_fixture
async def async_pk_table(
    async_dynamodb: AsyncDynamoDBServiceResource,
) -> AsyncGenerator[AsyncTable, None]:
    table = await async_dynamodb.create_table(
        TableName="AsyncPKTable",
        KeySchema=[
            {"AttributeName": "id", "KeyType": "HASH"},
        ],
        AttributeDefinitions=[
            {"AttributeName": "id", "AttributeType": "S"},
        ],
        BillingMode="PAY_PER_REQUEST",
    )
    await table.wait_until_exists()

    yield table

    await table.delete()


@async_fixture
async def async_pk_sk_table(
    async_dynamodb: AsyncDynamoDBServiceResource,
) -> AsyncGenerator[AsyncTable, None]:
    table = await async_dynamodb.create_table(
        TableName="AsyncPKSKTable",
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
    await table.wait_until_exists()

    yield table

    await table.delete()


@async_fixture
async def async_pk_sk_table_with_gsi(
    async_dynamodb: AsyncDynamoDBServiceResource,
) -> AsyncGenerator[AsyncTable, None]:
    table = await async_dynamodb.create_table(
        TableName="AsyncPKSKTableWithGSI",
        KeySchema=[
            {"AttributeName": "id", "KeyType": "HASH"},
            {"AttributeName": "sort", "KeyType": "RANGE"},
        ],
        AttributeDefinitions=[
            {"AttributeName": "id", "AttributeType": "S"},
            {"AttributeName": "sort", "AttributeType": "S"},
            {"AttributeName": "status", "AttributeType": "S"},
        ],
        GlobalSecondaryIndexes=[
            {
                "IndexName": "status-index",
                "KeySchema": [
                    {"AttributeName": "status", "KeyType": "HASH"},
                ],
                "Projection": {"ProjectionType": "ALL"},
            },
        ],
        BillingMode="PAY_PER_REQUEST",
    )
    await table.wait_until_exists()

    yield table

    await table.delete()


@async_fixture
async def async_pk_sk_table_with_lsi(
    async_dynamodb: AsyncDynamoDBServiceResource,
) -> AsyncGenerator[AsyncTable, None]:
    table = await async_dynamodb.create_table(
        TableName="AsyncPKSKTableWithLSI",
        KeySchema=[
            {"AttributeName": "id", "KeyType": "HASH"},
            {"AttributeName": "sort", "KeyType": "RANGE"},
        ],
        AttributeDefinitions=[
            {"AttributeName": "id", "AttributeType": "S"},
            {"AttributeName": "sort", "AttributeType": "S"},
            {"AttributeName": "created_at", "AttributeType": "S"},
        ],
        LocalSecondaryIndexes=[
            {
                "IndexName": "created-at-index",
                "KeySchema": [
                    {"AttributeName": "id", "KeyType": "HASH"},
                    {"AttributeName": "created_at", "KeyType": "RANGE"},
                ],
                "Projection": {"ProjectionType": "ALL"},
            },
        ],
        BillingMode="PAY_PER_REQUEST",
    )
    await table.wait_until_exists()

    yield table

    await table.delete()
