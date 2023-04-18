package edu.berkeley.cs186.database.concurrency;

import edu.berkeley.cs186.database.TransactionContext;
import java.util.*;

/**
 * LockUtil is a declarative layer which simplifies multigranularity lock
 * acquisition for the user (you, in the last task of Part 2). Generally
 * speaking, you should use LockUtil for lock acquisition instead of calling
 * LockContext methods directly.
 */
public class LockUtil {
    /**
     * Ensure that the current transaction can perform actions requiring
     * `requestType` on `lockContext`.
     *
     * `requestType` is guaranteed to be one of: S, X, NL.
     *
     * This method should promote/escalate/acquire as needed, but should only
     * grant the least permissive set of locks needed. We recommend that you
     * think about what to do in each of the following cases:
     * - The current lock type can effectively substitute the requested type
     * - The current lock type is IX and the requested lock is S
     * - The current lock type is an intent lock
     * - None of the above: In this case, consider what values the explicit
     *   lock type can be, and think about how ancestor looks will need to be
     *   acquired or changed.
     *
     * You may find it useful to create a helper method that ensures you have
     * the appropriate locks on all ancestors.
     */
    public static void ensureSufficientLockHeld(LockContext lockContext, LockType requestType) {
        // requestType must be S, X, or NL
        assert (requestType == LockType.S || requestType == LockType.X || requestType == LockType.NL);

        // Do nothing if the transaction or lockContext is null
        TransactionContext transaction = TransactionContext.getTransaction();
        if (transaction == null || lockContext == null) return;

        // You may find these variables useful
        LockContext parentContext = lockContext.parentContext();
        LockType effectiveLockType = lockContext.getEffectiveLockType(transaction);
        LockType explicitLockType = lockContext.getExplicitLockType(transaction);

        // TODO(proj4_part2): implement
        if (LockType.substitutable(effectiveLockType, requestType)) { //case 1
            return;
        }
        if (requestType.equals(LockType.S)) { //case 2
            ArrayList<LockContext> parentContxts = new ArrayList<>();
            LockContext currParent = lockContext.parent;
            while (currParent != null) {
                parentContxts.add(currParent);
                currParent = currParent.parent;
            }
            Collections.reverse(parentContxts);
            for (LockContext parent : parentContxts) {
                LockType parentLockType = parent.getExplicitLockType(transaction);
                if (parentLockType.equals(LockType.NL)) {
                    parent.acquire(transaction, LockType.IS);
                }
            }
            if (explicitLockType.equals(LockType.NL)) {
                lockContext.acquire(transaction, LockType.S);
            }
            else if (explicitLockType.equals(LockType.IS)) {
                lockContext.escalate(transaction);
            } else {
                lockContext.promote(transaction, LockType.SIX);
            }
        } else {
            ArrayList<LockContext> parents = new ArrayList<>();
            LockContext parent = lockContext.parent;
            while (parent != null) {
                parents.add(parent);
                parent = parent.parent;
            }
            Collections.reverse(parents);
            for (LockContext par : parents) {
                LockType parentLockType = par.getExplicitLockType(transaction);
                if (parentLockType.equals(LockType.NL)) {
                    par.acquire(transaction, LockType.IX);
                }
                else if (parentLockType.equals(LockType.IS)) {
                    par.promote(transaction, LockType.IX);
                }
                else if (parentLockType.equals(LockType.S)) {
                    par.promote(transaction, LockType.SIX);
                }
            }
            if (explicitLockType.equals(LockType.NL)) {
                lockContext.acquire(transaction, LockType.X);
            }
            else if (explicitLockType.equals(LockType.IS)) {
                lockContext.escalate(transaction);
                lockContext.promote(transaction, LockType.X);
            }
            else if (explicitLockType.equals(LockType.S)) {
                lockContext.promote(transaction, LockType.X);
            }
            else {
                lockContext.escalate(transaction);
            }
        }
        return;
    }

    // TODO(proj4_part2) add any helper methods you want
}
