from collections.abc import Generator
from os import environ

import boto3
from moto import mock_aws
from mypy_boto3_dynamodb.service_resource import Table
from pytest import fixture


@fixture(scope="session", autouse=True)
def aws_credentials():
    environ["AWS_ACCESS_KEY_ID"] = "testing"
    environ["AWS_SECRET_ACCESS_KEY"] = "testing"  # noqa: S105
    environ["AWS_SECURITY_TOKEN"] = "testing"  # noqa: S105
    environ["AWS_SESSION_TOKEN"] = "testing"  # noqa: S105
    environ["AWS_DEFAULT_REGION"] = "us-east-1"


@fixture
def pk_table(aws_credentials: None) -> Generator[Table, None, None]:
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
def pk_sk_table(aws_credentials: None) -> Generator[Table, None, None]:
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
def pk_sk_table_with_gsi(aws_credentials: None) -> Generator[Table, None, None]:
    """Table with PK+SK and a GSI on 'status' attribute."""
    with mock_aws():
        dynamodb = boto3.resource("dynamodb")
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
def pk_sk_table_with_lsi(aws_credentials: None) -> Generator[Table, None, None]:
    """Table with PK+SK and an LSI on 'created_at' attribute."""
    with mock_aws():
        dynamodb = boto3.resource("dynamodb")
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
