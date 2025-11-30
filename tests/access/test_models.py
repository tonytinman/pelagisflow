"""
Tests for access control data models.

Tests all data structures used in the access control subsystem.
"""

import pytest
from access.models import (
    UCPrivilege,
    PrivilegeIntent,
    ActualPrivilege,
    PrivilegeDelta,
    AccessControlResult
)


class TestUCPrivilege:
    """Test UCPrivilege enum."""

    def test_enum_values(self):
        """Test that enum has expected values."""
        assert UCPrivilege.SELECT.value == "SELECT"
        assert UCPrivilege.MODIFY.value == "MODIFY"
        assert UCPrivilege.ALL_PRIVILEGES.value == "ALL PRIVILEGES"

    def test_enum_from_string(self):
        """Test creating enum from string."""
        assert UCPrivilege("SELECT") == UCPrivilege.SELECT
        assert UCPrivilege("MODIFY") == UCPrivilege.MODIFY
        assert UCPrivilege("ALL PRIVILEGES") == UCPrivilege.ALL_PRIVILEGES

    def test_enum_invalid_value(self):
        """Test that invalid values raise ValueError."""
        with pytest.raises(ValueError):
            UCPrivilege("INVALID")


class TestPrivilegeIntent:
    """Test PrivilegeIntent dataclass."""

    def test_create_with_enum(self):
        """Test creating with UCPrivilege enum."""
        intent = PrivilegeIntent(
            table="catalog.schema.table1",
            ad_group="ad_grp_test",
            privilege=UCPrivilege.SELECT,
            reason="Test role"
        )

        assert intent.table == "catalog.schema.table1"
        assert intent.ad_group == "ad_grp_test"
        assert intent.privilege == UCPrivilege.SELECT
        assert intent.reason == "Test role"

    def test_create_with_string_privilege(self):
        """Test that string privilege is converted to enum."""
        intent = PrivilegeIntent(
            table="catalog.schema.table1",
            ad_group="ad_grp_test",
            privilege="SELECT",
            reason="Test role"
        )

        assert isinstance(intent.privilege, UCPrivilege)
        assert intent.privilege == UCPrivilege.SELECT

    def test_equality(self):
        """Test equality comparison."""
        intent1 = PrivilegeIntent(
            table="catalog.schema.table1",
            ad_group="ad_grp_test",
            privilege=UCPrivilege.SELECT,
            reason="Role A"
        )

        intent2 = PrivilegeIntent(
            table="catalog.schema.table1",
            ad_group="ad_grp_test",
            privilege=UCPrivilege.SELECT,
            reason="Role B"  # Different reason, should still be equal
        )

        assert intent1 == intent2

    def test_inequality_different_table(self):
        """Test inequality when table differs."""
        intent1 = PrivilegeIntent(
            table="catalog.schema.table1",
            ad_group="ad_grp_test",
            privilege=UCPrivilege.SELECT,
            reason="Test"
        )

        intent2 = PrivilegeIntent(
            table="catalog.schema.table2",
            ad_group="ad_grp_test",
            privilege=UCPrivilege.SELECT,
            reason="Test"
        )

        assert intent1 != intent2

    def test_inequality_different_group(self):
        """Test inequality when group differs."""
        intent1 = PrivilegeIntent(
            table="catalog.schema.table1",
            ad_group="ad_grp_test1",
            privilege=UCPrivilege.SELECT,
            reason="Test"
        )

        intent2 = PrivilegeIntent(
            table="catalog.schema.table1",
            ad_group="ad_grp_test2",
            privilege=UCPrivilege.SELECT,
            reason="Test"
        )

        assert intent1 != intent2

    def test_inequality_different_privilege(self):
        """Test inequality when privilege differs."""
        intent1 = PrivilegeIntent(
            table="catalog.schema.table1",
            ad_group="ad_grp_test",
            privilege=UCPrivilege.SELECT,
            reason="Test"
        )

        intent2 = PrivilegeIntent(
            table="catalog.schema.table1",
            ad_group="ad_grp_test",
            privilege=UCPrivilege.MODIFY,
            reason="Test"
        )

        assert intent1 != intent2

    def test_hashable(self):
        """Test that PrivilegeIntent can be used in sets."""
        intent1 = PrivilegeIntent(
            table="catalog.schema.table1",
            ad_group="ad_grp_test",
            privilege=UCPrivilege.SELECT,
            reason="Test"
        )

        intent2 = PrivilegeIntent(
            table="catalog.schema.table1",
            ad_group="ad_grp_test",
            privilege=UCPrivilege.SELECT,
            reason="Different reason"
        )

        # Same hash despite different reason
        assert hash(intent1) == hash(intent2)

        # Can be used in sets
        intent_set = {intent1, intent2}
        assert len(intent_set) == 1  # Deduplicated


class TestActualPrivilege:
    """Test ActualPrivilege dataclass."""

    def test_create_with_enum(self):
        """Test creating with UCPrivilege enum."""
        actual = ActualPrivilege(
            table="catalog.schema.table1",
            ad_group="ad_grp_test",
            privilege=UCPrivilege.SELECT
        )

        assert actual.table == "catalog.schema.table1"
        assert actual.ad_group == "ad_grp_test"
        assert actual.privilege == UCPrivilege.SELECT

    def test_create_with_string_privilege(self):
        """Test that string privilege is converted to enum."""
        actual = ActualPrivilege(
            table="catalog.schema.table1",
            ad_group="ad_grp_test",
            privilege="SELECT"
        )

        assert isinstance(actual.privilege, UCPrivilege)
        assert actual.privilege == UCPrivilege.SELECT

    def test_equality(self):
        """Test equality comparison."""
        actual1 = ActualPrivilege(
            table="catalog.schema.table1",
            ad_group="ad_grp_test",
            privilege=UCPrivilege.SELECT
        )

        actual2 = ActualPrivilege(
            table="catalog.schema.table1",
            ad_group="ad_grp_test",
            privilege=UCPrivilege.SELECT
        )

        assert actual1 == actual2

    def test_hashable(self):
        """Test that ActualPrivilege can be used in sets."""
        actual1 = ActualPrivilege(
            table="catalog.schema.table1",
            ad_group="ad_grp_test",
            privilege=UCPrivilege.SELECT
        )

        actual2 = ActualPrivilege(
            table="catalog.schema.table1",
            ad_group="ad_grp_test",
            privilege=UCPrivilege.SELECT
        )

        assert hash(actual1) == hash(actual2)

        actual_set = {actual1, actual2}
        assert len(actual_set) == 1


class TestPrivilegeDelta:
    """Test PrivilegeDelta dataclass."""

    def test_create_grant(self):
        """Test creating a GRANT delta."""
        delta = PrivilegeDelta(
            action="GRANT",
            table="catalog.schema.table1",
            ad_group="ad_grp_test",
            privilege=UCPrivilege.SELECT,
            reason="New role mapping"
        )

        assert delta.action == "GRANT"
        assert delta.table == "catalog.schema.table1"
        assert delta.ad_group == "ad_grp_test"
        assert delta.privilege == UCPrivilege.SELECT
        assert delta.reason == "New role mapping"

    def test_create_revoke(self):
        """Test creating a REVOKE delta."""
        delta = PrivilegeDelta(
            action="REVOKE",
            table="catalog.schema.table1",
            ad_group="ad_grp_test",
            privilege=UCPrivilege.SELECT,
            reason="Role removed"
        )

        assert delta.action == "REVOKE"

    def test_invalid_action(self):
        """Test that invalid action raises ValueError."""
        with pytest.raises(ValueError, match="Invalid action"):
            PrivilegeDelta(
                action="INVALID",
                table="catalog.schema.table1",
                ad_group="ad_grp_test",
                privilege=UCPrivilege.SELECT,
                reason="Test"
            )

    def test_string_privilege_conversion(self):
        """Test that string privilege is converted to enum."""
        delta = PrivilegeDelta(
            action="GRANT",
            table="catalog.schema.table1",
            ad_group="ad_grp_test",
            privilege="SELECT",
            reason="Test"
        )

        assert isinstance(delta.privilege, UCPrivilege)
        assert delta.privilege == UCPrivilege.SELECT

    def test_grant_sql(self):
        """Test SQL generation for GRANT."""
        delta = PrivilegeDelta(
            action="GRANT",
            table="catalog.schema.table1",
            ad_group="ad_grp_test",
            privilege=UCPrivilege.SELECT,
            reason="Test"
        )

        expected = "GRANT SELECT ON TABLE catalog.schema.table1 TO `ad_grp_test`"
        assert delta.sql == expected

    def test_revoke_sql(self):
        """Test SQL generation for REVOKE."""
        delta = PrivilegeDelta(
            action="REVOKE",
            table="catalog.schema.table1",
            ad_group="ad_grp_test",
            privilege=UCPrivilege.MODIFY,
            reason="Test"
        )

        expected = "REVOKE MODIFY ON TABLE catalog.schema.table1 FROM `ad_grp_test`"
        assert delta.sql == expected

    def test_all_privileges_sql(self):
        """Test SQL generation for ALL PRIVILEGES."""
        delta = PrivilegeDelta(
            action="GRANT",
            table="catalog.schema.table1",
            ad_group="ad_grp_admin",
            privilege=UCPrivilege.ALL_PRIVILEGES,
            reason="Admin role"
        )

        expected = "GRANT ALL PRIVILEGES ON TABLE catalog.schema.table1 TO `ad_grp_admin`"
        assert delta.sql == expected


class TestAccessControlResult:
    """Test AccessControlResult dataclass."""

    def test_create_successful_result(self):
        """Test creating a successful result."""
        result = AccessControlResult(
            table="catalog.schema.table1",
            intended_count=5,
            actual_count=3,
            no_change_count=3,
            grants_attempted=2,
            grants_succeeded=2,
            grants_failed=0,
            revokes_attempted=0,
            revokes_succeeded=0,
            revokes_failed=0,
            execution_time_seconds=1.5
        )

        assert result.is_successful
        assert result.total_changes == 2
        assert result.success_rate == 100.0

    def test_create_result_with_failures(self):
        """Test creating a result with failures."""
        result = AccessControlResult(
            table="catalog.schema.table1",
            intended_count=5,
            actual_count=5,
            no_change_count=3,
            grants_attempted=2,
            grants_succeeded=1,
            grants_failed=1,
            revokes_attempted=0,
            revokes_succeeded=0,
            revokes_failed=0,
            execution_time_seconds=1.5
        )

        assert not result.is_successful
        assert result.success_rate == 50.0

    def test_create_result_with_errors(self):
        """Test creating a result with errors."""
        result = AccessControlResult(
            table="catalog.schema.table1",
            intended_count=5,
            actual_count=5,
            no_change_count=5,
            grants_attempted=0,
            grants_succeeded=0,
            grants_failed=0,
            revokes_attempted=0,
            revokes_succeeded=0,
            revokes_failed=0,
            execution_time_seconds=0.1,
            errors=["Group not found"]
        )

        assert not result.is_successful
        assert len(result.errors) == 1

    def test_no_changes_needed(self):
        """Test result when no changes needed."""
        result = AccessControlResult(
            table="catalog.schema.table1",
            intended_count=5,
            actual_count=5,
            no_change_count=5,
            grants_attempted=0,
            grants_succeeded=0,
            grants_failed=0,
            revokes_attempted=0,
            revokes_succeeded=0,
            revokes_failed=0,
            execution_time_seconds=0.1
        )

        assert result.is_successful
        assert result.total_changes == 0
        assert result.success_rate == 100.0

    def test_grants_and_revokes(self):
        """Test result with both grants and revokes."""
        result = AccessControlResult(
            table="catalog.schema.table1",
            intended_count=4,
            actual_count=4,
            no_change_count=2,
            grants_attempted=2,
            grants_succeeded=2,
            grants_failed=0,
            revokes_attempted=2,
            revokes_succeeded=2,
            revokes_failed=0,
            execution_time_seconds=2.0
        )

        assert result.is_successful
        assert result.total_changes == 4
        assert result.success_rate == 100.0

    def test_partial_success(self):
        """Test result with partial success."""
        result = AccessControlResult(
            table="catalog.schema.table1",
            intended_count=6,
            actual_count=6,
            no_change_count=2,
            grants_attempted=3,
            grants_succeeded=2,
            grants_failed=1,
            revokes_attempted=3,
            revokes_succeeded=2,
            revokes_failed=1,
            execution_time_seconds=2.5
        )

        assert not result.is_successful
        assert result.total_changes == 6
        assert result.success_rate == pytest.approx(66.67, rel=0.01)

    def test_string_representation(self):
        """Test string representation."""
        result = AccessControlResult(
            table="catalog.schema.table1",
            intended_count=5,
            actual_count=3,
            no_change_count=3,
            grants_attempted=2,
            grants_succeeded=2,
            grants_failed=0,
            revokes_attempted=0,
            revokes_succeeded=0,
            revokes_failed=0,
            execution_time_seconds=1.5
        )

        result_str = str(result)
        assert "catalog.schema.table1" in result_str
        assert "GRANTs=2/2" in result_str
        assert "REVOKEs=0/0" in result_str
        assert "Success=100.0%" in result_str
