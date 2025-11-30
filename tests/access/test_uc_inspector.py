"""
Tests for Unity Catalog privilege inspector.

Tests querying Unity Catalog to determine actual privilege state.
Uses mocks for Spark session since we don't have actual UC access in tests.
"""

import pytest
from unittest.mock import Mock, MagicMock
from access.uc_inspector import UCPrivilegeInspector
from access.models import ActualPrivilege, UCPrivilege


@pytest.fixture
def mock_spark():
    """Create a mock Spark session."""
    return Mock()


@pytest.fixture
def inspector(mock_spark):
    """Create a UC privilege inspector with mock Spark."""
    return UCPrivilegeInspector(mock_spark)


class MockRow:
    """Mock DataFrame row."""

    def __init__(self, principal, principal_type, action_type, object_type, object_key):
        self.principal = principal
        self.principal_type = principal_type
        self.action_type = action_type
        self.object_type = object_type
        self.object_key = object_key


class TestUCPrivilegeInspector:
    """Test UCPrivilegeInspector class."""

    def test_initialization(self, mock_spark):
        """Test inspector initialization."""
        inspector = UCPrivilegeInspector(mock_spark)
        assert inspector.spark == mock_spark

    def test_get_actual_privileges_single_group(self, inspector, mock_spark):
        """Test getting actual privileges for a table with one group."""
        # Mock SHOW GRANTS result
        mock_df = Mock()
        mock_df.collect.return_value = [
            MockRow(
                principal="ad_grp_test",
                principal_type="GROUP",
                action_type="SELECT",
                object_type="TABLE",
                object_key="catalog.schema.table1"
            )
        ]

        mock_spark.sql.return_value = mock_df

        actuals = inspector.get_actual_privileges(
            catalog="catalog",
            schema="schema",
            table="table1"
        )

        # Verify SQL was called
        mock_spark.sql.assert_called_once_with("SHOW GRANTS ON TABLE catalog.schema.table1")

        # Verify results
        assert len(actuals) == 1
        assert actuals[0].table == "catalog.schema.table1"
        assert actuals[0].ad_group == "ad_grp_test"
        assert actuals[0].privilege == UCPrivilege.SELECT

    def test_get_actual_privileges_multiple_groups(self, inspector, mock_spark):
        """Test getting privileges for multiple groups."""
        mock_df = Mock()
        mock_df.collect.return_value = [
            MockRow("ad_grp_test1", "GROUP", "SELECT", "TABLE", "catalog.schema.table1"),
            MockRow("ad_grp_test2", "GROUP", "SELECT", "TABLE", "catalog.schema.table1"),
            MockRow("ad_grp_admin", "GROUP", "MODIFY", "TABLE", "catalog.schema.table1"),
        ]

        mock_spark.sql.return_value = mock_df

        actuals = inspector.get_actual_privileges("catalog", "schema", "table1")

        assert len(actuals) == 3

        groups = {a.ad_group for a in actuals}
        assert groups == {"ad_grp_test1", "ad_grp_test2", "ad_grp_admin"}

    def test_get_actual_privileges_filters_users(self, inspector, mock_spark):
        """Test that USER principals are filtered out."""
        mock_df = Mock()
        mock_df.collect.return_value = [
            MockRow("ad_grp_test", "GROUP", "SELECT", "TABLE", "catalog.schema.table1"),
            MockRow("user@example.com", "USER", "SELECT", "TABLE", "catalog.schema.table1"),
            MockRow("service_principal", "SERVICE_PRINCIPAL", "SELECT", "TABLE", "catalog.schema.table1"),
        ]

        mock_spark.sql.return_value = mock_df

        actuals = inspector.get_actual_privileges("catalog", "schema", "table1")

        # Should only include GROUP principal
        assert len(actuals) == 1
        assert actuals[0].ad_group == "ad_grp_test"

    def test_get_actual_privileges_filters_unmanaged_privileges(self, inspector, mock_spark):
        """Test that unmanaged privileges are filtered out."""
        mock_df = Mock()
        mock_df.collect.return_value = [
            MockRow("ad_grp_test", "GROUP", "SELECT", "TABLE", "catalog.schema.table1"),
            MockRow("ad_grp_admin", "GROUP", "OWNERSHIP", "TABLE", "catalog.schema.table1"),
            MockRow("ad_grp_test", "GROUP", "USE SCHEMA", "SCHEMA", "catalog.schema"),
        ]

        mock_spark.sql.return_value = mock_df

        actuals = inspector.get_actual_privileges("catalog", "schema", "table1")

        # Should only include SELECT (managed privilege)
        # OWNERSHIP and USE SCHEMA are not in UCPrivilege enum
        assert len(actuals) == 1
        assert actuals[0].privilege == UCPrivilege.SELECT

    def test_get_actual_privileges_all_privilege_types(self, inspector, mock_spark):
        """Test all managed privilege types."""
        mock_df = Mock()
        mock_df.collect.return_value = [
            MockRow("ad_grp_reader", "GROUP", "SELECT", "TABLE", "catalog.schema.table1"),
            MockRow("ad_grp_writer", "GROUP", "MODIFY", "TABLE", "catalog.schema.table1"),
            MockRow("ad_grp_admin", "GROUP", "ALL PRIVILEGES", "TABLE", "catalog.schema.table1"),
        ]

        mock_spark.sql.return_value = mock_df

        actuals = inspector.get_actual_privileges("catalog", "schema", "table1")

        assert len(actuals) == 3

        privileges = {a.privilege for a in actuals}
        assert privileges == {
            UCPrivilege.SELECT,
            UCPrivilege.MODIFY,
            UCPrivilege.ALL_PRIVILEGES
        }

    def test_get_actual_privileges_table_not_exists(self, inspector, mock_spark):
        """Test handling table that doesn't exist."""
        # Simulate exception when table doesn't exist
        mock_spark.sql.side_effect = Exception("Table not found")

        actuals = inspector.get_actual_privileges("catalog", "schema", "nonexistent")

        # Should return empty list instead of raising
        assert len(actuals) == 0

    def test_get_actual_privileges_no_grants(self, inspector, mock_spark):
        """Test table with no grants."""
        mock_df = Mock()
        mock_df.collect.return_value = []

        mock_spark.sql.return_value = mock_df

        actuals = inspector.get_actual_privileges("catalog", "schema", "table1")

        assert len(actuals) == 0

    def test_table_exists_true(self, inspector, mock_spark):
        """Test checking if table exists - returns True."""
        mock_df = Mock()
        mock_spark.sql.return_value = mock_df

        exists = inspector.table_exists("catalog", "schema", "table1")

        assert exists
        mock_spark.sql.assert_called_once_with("DESCRIBE TABLE catalog.schema.table1")

    def test_table_exists_false(self, inspector, mock_spark):
        """Test checking if table exists - returns False."""
        mock_spark.sql.side_effect = Exception("Table not found")

        exists = inspector.table_exists("catalog", "schema", "nonexistent")

        assert not exists

    def test_get_workspace_groups_success(self, inspector, mock_spark):
        """Test getting workspace groups."""
        mock_df = Mock()
        mock_df.collect.return_value = [
            Mock(groupName="ad_grp_test1"),
            Mock(groupName="ad_grp_test2"),
            Mock(groupName="ad_grp_admin"),
        ]

        mock_spark.sql.return_value = mock_df

        groups = inspector.get_workspace_groups()

        assert len(groups) == 3
        assert "ad_grp_test1" in groups
        assert "ad_grp_test2" in groups
        assert "ad_grp_admin" in groups

        mock_spark.sql.assert_called_once_with("SHOW GROUPS")

    def test_get_workspace_groups_not_available(self, inspector, mock_spark):
        """Test getting groups when command not available."""
        mock_spark.sql.side_effect = Exception("Command not available")

        groups = inspector.get_workspace_groups()

        # Should return empty list instead of raising
        assert len(groups) == 0

    def test_get_workspace_groups_empty(self, inspector, mock_spark):
        """Test getting groups when no groups exist."""
        mock_df = Mock()
        mock_df.collect.return_value = []

        mock_spark.sql.return_value = mock_df

        groups = inspector.get_workspace_groups()

        assert len(groups) == 0

    def test_qualified_table_name(self, inspector, mock_spark):
        """Test that qualified table names are constructed correctly."""
        mock_df = Mock()
        mock_df.collect.return_value = []
        mock_spark.sql.return_value = mock_df

        inspector.get_actual_privileges("my_catalog", "my_schema", "my_table")

        # Verify SQL was called with correct qualified name
        expected_sql = "SHOW GRANTS ON TABLE my_catalog.my_schema.my_table"
        mock_spark.sql.assert_called_once_with(expected_sql)

    def test_multiple_privileges_same_group(self, inspector, mock_spark):
        """Test when same group has multiple privileges."""
        mock_df = Mock()
        mock_df.collect.return_value = [
            MockRow("ad_grp_writer", "GROUP", "SELECT", "TABLE", "catalog.schema.table1"),
            MockRow("ad_grp_writer", "GROUP", "MODIFY", "TABLE", "catalog.schema.table1"),
        ]

        mock_spark.sql.return_value = mock_df

        actuals = inspector.get_actual_privileges("catalog", "schema", "table1")

        # Should return both privileges as separate ActualPrivilege objects
        assert len(actuals) == 2

        writer_actuals = [a for a in actuals if a.ad_group == "ad_grp_writer"]
        assert len(writer_actuals) == 2

        privileges = {a.privilege for a in writer_actuals}
        assert privileges == {UCPrivilege.SELECT, UCPrivilege.MODIFY}

    def test_actual_privilege_immutability(self, inspector, mock_spark):
        """Test that ActualPrivilege objects are correctly created."""
        mock_df = Mock()
        mock_df.collect.return_value = [
            MockRow("ad_grp_test", "GROUP", "SELECT", "TABLE", "catalog.schema.table1"),
        ]

        mock_spark.sql.return_value = mock_df

        actuals = inspector.get_actual_privileges("catalog", "schema", "table1")

        actual = actuals[0]

        # Verify all fields are set correctly
        assert isinstance(actual, ActualPrivilege)
        assert actual.table == "catalog.schema.table1"
        assert actual.ad_group == "ad_grp_test"
        assert actual.privilege == UCPrivilege.SELECT

    def test_case_sensitivity_in_principals(self, inspector, mock_spark):
        """Test that principal names preserve case."""
        mock_df = Mock()
        mock_df.collect.return_value = [
            MockRow("AD_GRP_TEST", "GROUP", "SELECT", "TABLE", "catalog.schema.table1"),
            MockRow("ad_grp_test", "GROUP", "MODIFY", "TABLE", "catalog.schema.table1"),
        ]

        mock_spark.sql.return_value = mock_df

        actuals = inspector.get_actual_privileges("catalog", "schema", "table1")

        # Should preserve case in group names
        groups = {a.ad_group for a in actuals}
        assert groups == {"AD_GRP_TEST", "ad_grp_test"}

    def test_error_handling_show_grants(self, inspector, mock_spark):
        """Test various error scenarios with SHOW GRANTS."""
        # Test different exception types
        for error in [
            Exception("Table not found"),
            RuntimeError("Permission denied"),
            ValueError("Invalid catalog")
        ]:
            mock_spark.sql.side_effect = error

            actuals = inspector.get_actual_privileges("catalog", "schema", "table1")

            # Should always return empty list on error
            assert len(actuals) == 0
