"""
Tests for privilege delta generator.

Tests the differential analysis that compares intended vs actual privileges
and generates required changes.
"""

import pytest
from access.delta_generator import PrivilegeDeltaGenerator
from access.models import PrivilegeIntent, ActualPrivilege, PrivilegeDelta, UCPrivilege


@pytest.fixture
def generator():
    """Create a delta generator."""
    return PrivilegeDeltaGenerator()


class TestPrivilegeDeltaGenerator:
    """Test PrivilegeDeltaGenerator class."""

    def test_no_intents_no_actuals(self, generator):
        """Test with no intents and no actuals."""
        deltas, no_change = generator.generate_deltas([], [])

        assert len(deltas) == 0
        assert no_change == 0

    def test_intents_match_actuals_exactly(self, generator):
        """Test when intents match actuals exactly."""
        intents = [
            PrivilegeIntent(
                table="catalog.schema.table1",
                ad_group="ad_grp_test",
                privilege=UCPrivilege.SELECT,
                reason="Role: consumer"
            )
        ]

        actuals = [
            ActualPrivilege(
                table="catalog.schema.table1",
                ad_group="ad_grp_test",
                privilege=UCPrivilege.SELECT
            )
        ]

        deltas, no_change = generator.generate_deltas(intents, actuals)

        assert len(deltas) == 0
        assert no_change == 1

    def test_grant_needed_new_privilege(self, generator):
        """Test generating GRANT when privilege is in intent but not actual."""
        intents = [
            PrivilegeIntent(
                table="catalog.schema.table1",
                ad_group="ad_grp_test",
                privilege=UCPrivilege.SELECT,
                reason="Role: consumer"
            )
        ]

        actuals = []

        deltas, no_change = generator.generate_deltas(intents, actuals)

        assert len(deltas) == 1
        assert no_change == 0

        delta = deltas[0]
        assert delta.action == "GRANT"
        assert delta.table == "catalog.schema.table1"
        assert delta.ad_group == "ad_grp_test"
        assert delta.privilege == UCPrivilege.SELECT
        assert delta.reason == "Role: consumer"

    def test_revoke_needed_removed_privilege(self, generator):
        """Test generating REVOKE when privilege is in actual but not intent."""
        intents = []

        actuals = [
            ActualPrivilege(
                table="catalog.schema.table1",
                ad_group="ad_grp_old",
                privilege=UCPrivilege.SELECT
            )
        ]

        deltas, no_change = generator.generate_deltas(intents, actuals)

        assert len(deltas) == 1
        assert no_change == 0

        delta = deltas[0]
        assert delta.action == "REVOKE"
        assert delta.table == "catalog.schema.table1"
        assert delta.ad_group == "ad_grp_old"
        assert delta.privilege == UCPrivilege.SELECT
        assert delta.reason == "Not in current metadata - removing"

    def test_mixed_grant_revoke_nochange(self, generator):
        """Test with a mix of grants, revokes, and no-change."""
        intents = [
            # Should stay (in both)
            PrivilegeIntent(
                table="catalog.schema.table1",
                ad_group="ad_grp_keep",
                privilege=UCPrivilege.SELECT,
                reason="Role: consumer"
            ),
            # Should grant (new)
            PrivilegeIntent(
                table="catalog.schema.table1",
                ad_group="ad_grp_new",
                privilege=UCPrivilege.SELECT,
                reason="Role: consumer"
            ),
        ]

        actuals = [
            # Should stay (in both)
            ActualPrivilege(
                table="catalog.schema.table1",
                ad_group="ad_grp_keep",
                privilege=UCPrivilege.SELECT
            ),
            # Should revoke (removed)
            ActualPrivilege(
                table="catalog.schema.table1",
                ad_group="ad_grp_old",
                privilege=UCPrivilege.SELECT
            ),
        ]

        deltas, no_change = generator.generate_deltas(intents, actuals)

        assert len(deltas) == 2
        assert no_change == 1

        grants = [d for d in deltas if d.action == "GRANT"]
        revokes = [d for d in deltas if d.action == "REVOKE"]

        assert len(grants) == 1
        assert len(revokes) == 1

        assert grants[0].ad_group == "ad_grp_new"
        assert revokes[0].ad_group == "ad_grp_old"

    def test_multiple_privileges_same_group(self, generator):
        """Test when same group has multiple privileges."""
        intents = [
            PrivilegeIntent(
                table="catalog.schema.table1",
                ad_group="ad_grp_writer",
                privilege=UCPrivilege.SELECT,
                reason="Role: writer"
            ),
            PrivilegeIntent(
                table="catalog.schema.table1",
                ad_group="ad_grp_writer",
                privilege=UCPrivilege.MODIFY,
                reason="Role: writer"
            ),
        ]

        actuals = [
            ActualPrivilege(
                table="catalog.schema.table1",
                ad_group="ad_grp_writer",
                privilege=UCPrivilege.SELECT
            )
        ]

        deltas, no_change = generator.generate_deltas(intents, actuals)

        # Should grant MODIFY, keep SELECT
        assert len(deltas) == 1
        assert no_change == 1

        delta = deltas[0]
        assert delta.action == "GRANT"
        assert delta.privilege == UCPrivilege.MODIFY

    def test_different_tables(self, generator):
        """Test with privileges for different tables."""
        intents = [
            PrivilegeIntent(
                table="catalog.schema.table1",
                ad_group="ad_grp_test",
                privilege=UCPrivilege.SELECT,
                reason="Role: consumer"
            ),
            PrivilegeIntent(
                table="catalog.schema.table2",
                ad_group="ad_grp_test",
                privilege=UCPrivilege.SELECT,
                reason="Role: consumer"
            ),
        ]

        actuals = [
            ActualPrivilege(
                table="catalog.schema.table1",
                ad_group="ad_grp_test",
                privilege=UCPrivilege.SELECT
            )
        ]

        deltas, no_change = generator.generate_deltas(intents, actuals)

        # Should grant for table2, keep table1
        assert len(deltas) == 1
        assert no_change == 1

        delta = deltas[0]
        assert delta.table == "catalog.schema.table2"

    def test_group_by_action(self, generator):
        """Test grouping deltas by action."""
        deltas = [
            PrivilegeDelta(
                action="GRANT",
                table="catalog.schema.table1",
                ad_group="ad_grp_new1",
                privilege=UCPrivilege.SELECT,
                reason="Test"
            ),
            PrivilegeDelta(
                action="GRANT",
                table="catalog.schema.table1",
                ad_group="ad_grp_new2",
                privilege=UCPrivilege.SELECT,
                reason="Test"
            ),
            PrivilegeDelta(
                action="REVOKE",
                table="catalog.schema.table1",
                ad_group="ad_grp_old1",
                privilege=UCPrivilege.SELECT,
                reason="Test"
            ),
        ]

        grants, revokes = generator.group_by_action(deltas)

        assert len(grants) == 2
        assert len(revokes) == 1

        assert all(d.action == "GRANT" for d in grants)
        assert all(d.action == "REVOKE" for d in revokes)

    def test_group_by_action_empty(self, generator):
        """Test grouping with empty deltas."""
        grants, revokes = generator.group_by_action([])

        assert len(grants) == 0
        assert len(revokes) == 0

    def test_summarize_deltas(self, generator):
        """Test summarizing deltas."""
        deltas = [
            PrivilegeDelta(
                action="GRANT",
                table="catalog.schema.table1",
                ad_group="ad_grp_test1",
                privilege=UCPrivilege.SELECT,
                reason="Test"
            ),
            PrivilegeDelta(
                action="GRANT",
                table="catalog.schema.table1",
                ad_group="ad_grp_test2",
                privilege=UCPrivilege.SELECT,
                reason="Test"
            ),
            PrivilegeDelta(
                action="GRANT",
                table="catalog.schema.table1",
                ad_group="ad_grp_test1",
                privilege=UCPrivilege.MODIFY,
                reason="Test"
            ),
            PrivilegeDelta(
                action="REVOKE",
                table="catalog.schema.table1",
                ad_group="ad_grp_old",
                privilege=UCPrivilege.SELECT,
                reason="Test"
            ),
        ]

        summary = generator.summarize_deltas(deltas)

        assert summary["total"] == 4
        assert summary["grants"] == 3
        assert summary["revokes"] == 1
        assert summary["by_privilege"]["SELECT"] == 3
        assert summary["by_privilege"]["MODIFY"] == 1
        assert summary["by_group"]["ad_grp_test1"] == 2
        assert summary["by_group"]["ad_grp_test2"] == 1
        assert summary["by_group"]["ad_grp_old"] == 1

    def test_summarize_deltas_empty(self, generator):
        """Test summarizing empty deltas."""
        summary = generator.summarize_deltas([])

        assert summary["total"] == 0
        assert summary["grants"] == 0
        assert summary["revokes"] == 0
        assert len(summary["by_privilege"]) == 0
        assert len(summary["by_group"]) == 0

    def test_all_privileges_enum(self, generator):
        """Test with ALL PRIVILEGES enum."""
        intents = [
            PrivilegeIntent(
                table="catalog.schema.table1",
                ad_group="ad_grp_admin",
                privilege=UCPrivilege.ALL_PRIVILEGES,
                reason="Role: admin"
            )
        ]

        actuals = []

        deltas, no_change = generator.generate_deltas(intents, actuals)

        assert len(deltas) == 1
        delta = deltas[0]
        assert delta.privilege == UCPrivilege.ALL_PRIVILEGES

    def test_large_dataset(self, generator):
        """Test with large dataset to verify performance."""
        # Create 100 intents and 100 actuals with 50% overlap
        intents = [
            PrivilegeIntent(
                table=f"catalog.schema.table{i}",
                ad_group="ad_grp_test",
                privilege=UCPrivilege.SELECT,
                reason="Role: consumer"
            )
            for i in range(100)
        ]

        actuals = [
            ActualPrivilege(
                table=f"catalog.schema.table{i}",
                ad_group="ad_grp_test",
                privilege=UCPrivilege.SELECT
            )
            for i in range(50, 150)  # 50-99 overlap with intents
        ]

        deltas, no_change = generator.generate_deltas(intents, actuals)

        # Should have 50 grants (0-49), 50 revokes (100-149), 50 no change (50-99)
        assert no_change == 50

        grants = [d for d in deltas if d.action == "GRANT"]
        revokes = [d for d in deltas if d.action == "REVOKE"]

        assert len(grants) == 50
        assert len(revokes) == 50

    def test_duplicate_intents_deduplicated(self, generator):
        """Test that duplicate intents are deduplicated via set."""
        intents = [
            PrivilegeIntent(
                table="catalog.schema.table1",
                ad_group="ad_grp_test",
                privilege=UCPrivilege.SELECT,
                reason="Role: consumer"
            ),
            PrivilegeIntent(
                table="catalog.schema.table1",
                ad_group="ad_grp_test",
                privilege=UCPrivilege.SELECT,
                reason="Role: consumer.general"  # Different reason, same privilege
            ),
        ]

        actuals = []

        deltas, no_change = generator.generate_deltas(intents, actuals)

        # Should only generate one GRANT (duplicates removed by set)
        assert len(deltas) == 1
        assert no_change == 0

    def test_string_privilege_in_delta(self, generator):
        """Test that string privileges work in intents/actuals."""
        intents = [
            PrivilegeIntent(
                table="catalog.schema.table1",
                ad_group="ad_grp_test",
                privilege="SELECT",  # String instead of enum
                reason="Role: consumer"
            )
        ]

        actuals = []

        deltas, no_change = generator.generate_deltas(intents, actuals)

        assert len(deltas) == 1
        assert deltas[0].privilege == UCPrivilege.SELECT
