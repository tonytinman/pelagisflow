"""
Privilege delta generator.

Compares intended privileges vs actual privileges to generate
the changes needed (GRANTs and REVOKEs).
"""

from typing import List, Tuple
from .models import PrivilegeIntent, ActualPrivilege, PrivilegeDelta


class PrivilegeDeltaGenerator:
    """
    Generate privilege deltas by comparing intent vs actual.
    
    Implements differential analysis:
    - In intent but not actual → GRANT needed
    - In actual but not intent → REVOKE needed
    - In both → No action (already correct)
    
    Usage:
        generator = PrivilegeDeltaGenerator()
        
        deltas, no_change = generator.generate_deltas(
            intents=[...],
            actuals=[...]
        )
    """
    
    def generate_deltas(
        self,
        intents: List[PrivilegeIntent],
        actuals: List[ActualPrivilege]
    ) -> Tuple[List[PrivilegeDelta], int]:
        """
        Compare intended vs actual and generate required changes.
        
        This is the core differential analysis logic.
        
        Args:
            intents: Desired privileges (from metadata)
            actuals: Current privileges (from UC)
            
        Returns:
            Tuple of (deltas, no_change_count)
            - deltas: List of PrivilegeDelta (changes to make)
            - no_change_count: Number of privileges already correct
            
        Example:
            intents = [
                PrivilegeIntent(table="t1", group="g1", privilege=SELECT),
                PrivilegeIntent(table="t1", group="g2", privilege=SELECT)
            ]
            
            actuals = [
                ActualPrivilege(table="t1", group="g1", privilege=SELECT),
                ActualPrivilege(table="t1", group="g3", privilege=SELECT)
            ]
            
            deltas, no_change = generator.generate_deltas(intents, actuals)
            
            # Results:
            # deltas = [
            #   PrivilegeDelta(GRANT, table="t1", group="g2", ...)  # New
            #   PrivilegeDelta(REVOKE, table="t1", group="g3", ...) # Remove
            # ]
            # no_change = 1  # g1 already correct
        """
        deltas = []
        
        # Convert to sets for efficient comparison
        # Uses __hash__ and __eq__ from PrivilegeIntent/ActualPrivilege
        intent_set = set(intents)
        actual_set = set(actuals)
        
        # Find GRANTs needed (in intent but not actual)
        to_grant = intent_set - actual_set
        
        for intent in to_grant:
            deltas.append(PrivilegeDelta(
                action="GRANT",
                table=intent.table,
                ad_group=intent.ad_group,
                privilege=intent.privilege,
                reason=intent.reason
            ))
        
        # Find REVOKEs needed (in actual but not intent)
        to_revoke = actual_set - intent_set
        
        for actual in to_revoke:
            deltas.append(PrivilegeDelta(
                action="REVOKE",
                table=actual.table,
                ad_group=actual.ad_group,
                privilege=actual.privilege,
                reason="Not in current metadata - removing"
            ))
        
        # Count privileges already correct (in both sets)
        no_change_count = len(intent_set & actual_set)
        
        return deltas, no_change_count
    
    def group_by_action(
        self,
        deltas: List[PrivilegeDelta]
    ) -> Tuple[List[PrivilegeDelta], List[PrivilegeDelta]]:
        """
        Separate deltas into GRANTs and REVOKEs.
        
        Useful for logging and reporting.
        
        Args:
            deltas: List of privilege deltas
            
        Returns:
            Tuple of (grants, revokes)
            
        Example:
            grants, revokes = generator.group_by_action(deltas)
            print(f"Need to GRANT {len(grants)} privileges")
            print(f"Need to REVOKE {len(revokes)} privileges")
        """
        grants = [d for d in deltas if d.action == "GRANT"]
        revokes = [d for d in deltas if d.action == "REVOKE"]
        return grants, revokes
    
    def summarize_deltas(
        self,
        deltas: List[PrivilegeDelta]
    ) -> dict:
        """
        Generate summary statistics for deltas.
        
        Args:
            deltas: List of privilege deltas
            
        Returns:
            Dict with summary statistics
            
        Example:
            summary = generator.summarize_deltas(deltas)
            # Returns: {
            #   "total": 10,
            #   "grants": 6,
            #   "revokes": 4,
            #   "by_privilege": {
            #       "SELECT": 8,
            #       "MODIFY": 2
            #   },
            #   "by_group": {
            #       "ad_grp_finance_dev": 5,
            #       "ad_grp_payments_dev": 5
            #   }
            # }
        """
        summary = {
            "total": len(deltas),
            "grants": sum(1 for d in deltas if d.action == "GRANT"),
            "revokes": sum(1 for d in deltas if d.action == "REVOKE"),
            "by_privilege": {},
            "by_group": {}
        }
        
        # Count by privilege
        for delta in deltas:
            priv = delta.privilege.value
            summary["by_privilege"][priv] = summary["by_privilege"].get(priv, 0) + 1
        
        # Count by group
        for delta in deltas:
            group = delta.ad_group
            summary["by_group"][group] = summary["by_group"].get(group, 0) + 1
        
        return summary