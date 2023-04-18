package edu.berkeley.cs186.database.concurrency;

import edu.berkeley.cs186.database.TransactionContext;



import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

/**
 * LockContext wraps around LockManager to provide the hierarchical structure
 * of multigranularity locking. Calls to acquire/release/etc. locks should
 * be mostly done through a LockContext, which provides access to locking
 * methods at a certain point in the hierarchy (database, table X, etc.)
 */
public class LockContext {
    // You should not remove any of these fields. You may add additional
    // fields/methods as you see fit.

    // The underlying lock manager.
    protected final LockManager lockman;

    // The parent LockContext object, or null if this LockContext is at the top of the hierarchy.
    protected final LockContext parent;

    // The name of the resource this LockContext represents.
    protected ResourceName name;

    // Whether this LockContext is readonly. If a LockContext is readonly, acquire/release/promote/escalate should
    // throw an UnsupportedOperationException.
    protected boolean readonly;

    // A mapping between transaction numbers, and the number of locks on children of this LockContext
    // that the transaction holds.
    protected final Map<Long, Integer> numChildLocks;

    // You should not modify or use this directly.
    protected final Map<String, LockContext> children;

    // Whether or not any new child LockContexts should be marked readonly.
    protected boolean childLocksDisabled;

    public LockContext(LockManager lockman, LockContext parent, String name) {
        this(lockman, parent, name, false);
    }

    protected LockContext(LockManager lockman, LockContext parent, String name,
                          boolean readonly) {
        this.lockman = lockman;
        this.parent = parent;
        if (parent == null) {
            this.name = new ResourceName(name);
        } else {
            this.name = new ResourceName(parent.getResourceName(), name);
        }
        this.readonly = readonly;
        this.numChildLocks = new ConcurrentHashMap<>();
        this.children = new ConcurrentHashMap<>();
        this.childLocksDisabled = readonly;
    }

    /**
     * Gets a lock context corresponding to `name` from a lock manager.
     */
    public static LockContext fromResourceName(LockManager lockman, ResourceName name) {
        Iterator<String> names = name.getNames().iterator();
        LockContext ctx;
        String n1 = names.next();
        ctx = lockman.context(n1);
        while (names.hasNext()) {
            String n = names.next();
            ctx = ctx.childContext(n);
        }
        return ctx;
    }

    /**
     * Get the name of the resource that this lock context pertains to.
     */
    public ResourceName getResourceName() {
        return name;
    }

    /**
     * Acquire a `lockType` lock, for transaction `transaction`.
     *
     * Note: you must make any necessary updates to numChildLocks, or else calls
     * to LockContext#getNumChildren will not work properly.
     *
     * @throws InvalidLockException if the request is invalid
     * @throws DuplicateLockRequestException if a lock is already held by the
     * transaction.
     * @throws UnsupportedOperationException if context is readonly
     */
    public void acquire(TransactionContext transaction, LockType lockType)
            throws InvalidLockException, DuplicateLockRequestException {
        // TODO(proj4_part2): implement
        if (readonly) {
            throw new UnsupportedOperationException("Context is read only");
        }
        if (hasSIXAncestor(transaction) && (lockType.equals(LockType.S) || lockType.equals(LockType.IS))) {
            throw new InvalidLockException("The context contains SIX lock ancestor");
        }
        LockType explicitLockType = getExplicitLockType(transaction);
        LockType thisLockType = acquireHelper(transaction);

        if (explicitLockType.equals(lockType)) {
            throw new DuplicateLockRequestException("This lock is already held by the transaction.");
        }
        if (!LockType.canBeParentLock(thisLockType, lockType) && !thisLockType.equals(LockType.NL)) {
            throw new InvalidLockException("The lock types do not match.");
        }
        lockman.acquire(transaction, getResourceName(), lockType);
        addTxnParent(transaction, this); // traverse to the parent context
    }

    public void addTxnParent(TransactionContext transaction, LockContext lockContext) { // helper method for adding a lock to the parents
        if (lockContext.parent == null) {
            return;
        }
        int numLocks = lockContext.parentContext().numChildLocks.getOrDefault(transaction.getTransNum(), 0);
        lockContext.parentContext().numChildLocks.put(transaction.getTransNum(), numLocks + 1);
        addTxnParent(transaction, lockContext.parentContext());
    }

    /**
     * Release `transaction`'s lock on `name`.
     *
     * Note: you *must* make any necessary updates to numChildLocks, or
     * else calls to LockContext#getNumChildren will not work properly.
     *
     * @throws NoLockHeldException if no lock on `name` is held by `transaction`
     * @throws InvalidLockException if the lock cannot be released because
     * doing so would violate multigranularity locking constraints
     * @throws UnsupportedOperationException if context is readonly
     */
    public void release(TransactionContext transaction)
            throws NoLockHeldException, InvalidLockException {
        // TODO(proj4_part2): implement
        if (readonly) {
            throw new UnsupportedOperationException("The context is readonly.");
        }
        if (lockman.getLockType(transaction, getResourceName()).equals(LockType.NL)) {
            throw new NoLockHeldException("No lock on 'name' is held by transaction.");
        }
        int numLocks = numChildLocks.getOrDefault(transaction.getTransNum(), 0);
        if (numLocks != 0) { // bottom up
            throw new InvalidLockException("Lock cannot be released because it would violate multigranularity locking constraints.");
        }
        lockman.release(transaction, getResourceName());
        releaseHelper(transaction, this);
    }

    public void releaseHelper(TransactionContext transaction, LockContext lockContext) { // may have to edit because it starts with 'this' instead of parent
        if (lockContext.parentContext() == null) {
            return;
        }
        int numLocks = lockContext.parentContext().numChildLocks.getOrDefault(transaction.getTransNum(), 0);
        lockContext.parentContext().numChildLocks.put(transaction.getTransNum(), numLocks - 1);
        releaseHelper(transaction, lockContext.parentContext());
    }

    /**
     * Promote `transaction`'s lock to `newLockType`. For promotion to SIX from
     * IS/IX, all S and IS locks on descendants must be simultaneously
     * released. The helper function sisDescendants may be helpful here.
     *
     * Note: you *must* make any necessary updates to numChildLocks, or else
     * calls to LockContext#getNumChildren will not work properly.
     *
     * @throws DuplicateLockRequestException if `transaction` already has a
     * `newLockType` lock
     * @throws NoLockHeldException if `transaction` has no lock
     * @throws InvalidLockException if the requested lock type is not a
     * promotion or promoting would cause the lock manager to enter an invalid
     * state (e.g. IS(parent), X(child)). A promotion from lock type A to lock
     * type B is valid if B is substitutable for A and B is not equal to A, or
     * if B is SIX and A is IS/IX/S, and invalid otherwise. hasSIXAncestor may
     * be helpful here.
     * @throws UnsupportedOperationException if context is readonly
     */
    public void promote(TransactionContext transaction, LockType newLockType)
            throws DuplicateLockRequestException, NoLockHeldException, InvalidLockException {
        // TODO(proj4_part2): implement
        if (readonly) {
            throw new UnsupportedOperationException("The context is readonly.");
        }
        LockType thisLockType = lockman.getLockType(transaction, getResourceName());
        if (thisLockType.equals(LockType.NL)) {
            throw new NoLockHeldException("Xact has no lock.");
        }
        if (thisLockType.equals(newLockType)) {
            throw new DuplicateLockRequestException("Xact already has this locktype.");
        }
        if (newLockType.equals(LockType.SIX) && (thisLockType.equals(LockType.S)
                || thisLockType.equals(LockType.IS) || thisLockType.equals(LockType.IX))) { //Special case 1
            List<ResourceName> sisList = sisDescendants(transaction);
            List<LockContext> lockContextList = new ArrayList<>();
            for (ResourceName rName : sisList) {
                lockContextList.add(fromResourceName(lockman, rName));
            }
            lockman.acquireAndRelease(transaction, getResourceName(), newLockType, sisList);

            for (LockContext lockContexts : lockContextList) {
                lockContexts.releaseHelper(transaction, lockContexts);
            }
            return;
        }
        if (hasSIXAncestor(transaction) && newLockType.equals(LockType.SIX)) { // Special case 2
            throw new InvalidLockException("Ancestor contains a SIX lock.");
        }
        if (LockType.substitutable(newLockType, thisLockType)) {
            lockman.promote(transaction, getResourceName(), newLockType);
        } else {
            throw new InvalidLockException("The requested lock type is not a promotion or promoting would cause the lock manager to enter an invalid state (e.g. IS(parent), X(child)).");
        }
    }

    /**
     * Escalate `transaction`'s lock from descendants of this context to this
     * level, using either an S or X lock. There should be no descendant locks
     * after this call, and every operation valid on descendants of this context
     * before this call must still be valid. You should only make *one* mutating
     * call to the lock manager, and should only request information about
     * TRANSACTION from the lock manager.
     *
     * For example, if a transaction has the following locks:
     *
     *                    IX(database)
     *                    /         \
     *               IX(table1)    S(table2)
     *                /      \
     *    S(table1 page3)  X(table1 page5)
     *
     * then after table1Context.escalate(transaction) is called, we should have:
     *
     *                    IX(database)
     *                    /         \
     *               X(table1)     S(table2)
     *
     * You should not make any mutating calls if the locks held by the
     * transaction do not change (such as when you call escalate multiple times
     * in a row).
     *
     * Note: you *must* make any necessary updates to numChildLocks of all
     * relevant contexts, or else calls to LockContext#getNumChildren will not
     * work properly.
     *
     * @throws NoLockHeldException if `transaction` has no lock at this level
     * @throws UnsupportedOperationException if context is readonly
     */
    public void escalate(TransactionContext transaction) throws NoLockHeldException {
        // TODO(proj4_part2): implement
        if (readonly) {
            throw new UnsupportedOperationException("The context is readonly.");
        }
        LockType thisLockType = lockman.getLockType(transaction,getResourceName());
        if (thisLockType.equals(LockType.NL)) {
            throw new NoLockHeldException("Xact has no lock at this level.");
        }
        if (thisLockType.equals(LockType.X) || thisLockType.equals(LockType.S)) { // no reason to escalate here
            return;
        }
        List<ResourceName> children = escalateHelper2(transaction, thisLockType);
        List<LockContext> childrenContext = new ArrayList<>();
        childrenContext.add(this);
        for (ResourceName tempName : children) {
            childrenContext.add(fromResourceName(lockman, tempName));
        }
        if (thisLockType.equals(LockType.IS)) {
            //for (ResourceName tempName : children) {
            //lockman.release(transaction, tempName);
            //}
            //lockman.acquire(transaction, getResourceName(), LockType.S);
            lockman.acquireAndRelease(transaction, getResourceName(), LockType.S, children);
        }
        if (thisLockType.equals(LockType.IX) || thisLockType.equals(LockType.SIX)) { // case when Xact lock is IX or SIX
            //for (ResourceName tempName : children) {
            //lockman.release(transaction, tempName);
            //}
            //lockman.acquire(transaction, getResourceName(), LockType.X);
            lockman.acquireAndRelease(transaction, getResourceName(), LockType.X, children);
        }
        for (LockContext contexts : childrenContext) {
            contexts.releaseHelper(transaction, contexts);
        }
    }
    public List<ResourceName> escalateHelper2(TransactionContext transaction, LockType lockType) { // try to get ALL children
        List<ResourceName> retList = new ArrayList<>();
        List<Lock> lockList = lockman.getLocks(transaction);
        for (Lock loc : lockList) {
            if (!loc.lockType.equals(LockType.NL) && loc.name.isDescendantOf(getResourceName())) {
                ResourceName locName = loc.name;
                retList.add(locName);
            }
        }
        return retList;
    }

    public List<ResourceName> escalateHelper(TransactionContext transaction, LockType lockType) { // try to get all the non NL children
        List<ResourceName> retList = new ArrayList<>();
        List<Lock> lockList = lockman.getLocks(transaction);
        if (lockType.equals(LockType.IS)) {
            for (Lock loc : lockList) {
                if (loc.lockType.equals(LockType.S) || loc.lockType.equals(LockType.IS)) {
                    ResourceName locName = loc.name;
                    retList.add(locName);
                }
            }
        } else if (lockType.equals(LockType.IX)) {
            for (Lock loc: lockList) {
                if (loc.lockType.equals(LockType.X) && isAncestor(fromResourceName(lockman, loc.name), this)) {
                    ResourceName locName = loc.name;
                    retList.add(locName);
                } else if (loc.lockType.equals(LockType.SIX) && isAncestor(fromResourceName(lockman, loc.name), this)) {
                    ResourceName locName = loc.name;
                    retList.add(locName);
                }
            }
        } else if (lockType.equals(LockType.SIX)) {
            for (Lock loc : lockList) {
                if (loc.lockType.equals(LockType.X) && isAncestor(fromResourceName(lockman, loc.name), this)) {
                    ResourceName locName = loc.name;
                    retList.add(locName);
                } else if (loc.lockType.equals(LockType.IX) && isAncestor(fromResourceName(lockman, loc.name), this)) {
                    ResourceName locName = loc.name;
                    retList.add(locName);
                }
            }
        }
        return retList;
    }

    public boolean isAncestor(LockContext currContext, LockContext parentContext) {
        LockContext temp = currContext;
        while (temp != null) {
            if (temp.equals(parentContext)) {
                return true;
            }
            temp = temp.parentContext();
        }
        return false;
    }

    /**
     * Helper method to get a list of resourceNames of all locks that are S or
     * IS and are descendants of current context for the given transaction.
     * @param transaction the given transaction
     * @return a list of ResourceNames of descendants which the transaction
     * holds an S or IS lock.
     */
    private List<ResourceName> sisDescendants(TransactionContext transaction) {
        // TODO(proj4_part2): implement
        List<ResourceName> retList = new ArrayList<>();
        List<Lock> lockList = lockman.getLocks(transaction);
        for (Lock loc : lockList) {
            if (loc.lockType.equals(LockType.S) || loc.lockType.equals(LockType.IS)) {
                ResourceName locName = loc.name;
                LockContext tempLockContext = fromResourceName(lockman, locName);
                if (!tempLockContext.equals(this)) {
                    while (tempLockContext != null) {
                        if (tempLockContext.parentContext() != null && tempLockContext.parentContext().equals(this)) {
                            retList.add(locName);
                        }
                        tempLockContext = tempLockContext.parentContext();
                    }
                }
            }
        }
        return retList;
    }

    /**
     * Get the type of lock that `transaction` holds at this level, or NL if no
     * lock is held at this level.
     */
    public LockType getExplicitLockType(TransactionContext transaction) {
        if (transaction == null) return LockType.NL;
        // TODO(proj4_part2): implement
        return lockman.getLockType(transaction, getResourceName());
    }

    /**
     * Gets the type of lock that the transaction has at this level, either
     * implicitly (e.g. explicit S lock at higher level implies S lock at this
     * level) or explicitly. Returns NL if there is no explicit nor implicit
     * lock.
     */
    public LockType getEffectiveLockType(TransactionContext transaction) {
        if (transaction == null) return LockType.NL;
        // TODO(proj4_part2): implement
        LockType thisLockType = lockman.getLockType(transaction, getResourceName());
        if (!thisLockType.equals(LockType.NL)) { // explicit = effective if it has one already
            return thisLockType;
        }
        LockContext parent = parentContext();
        while (parent != null) {
            if (!parent.getExplicitLockType(transaction).equals(LockType.NL)) {
                LockType parentType = parent.getExplicitLockType(transaction);
                if (parentType.equals(LockType.SIX)) {
                    return LockType.S;
                }
                if (parentType.equals(LockType.IS) || parentType.equals(LockType.IX)) {
                    return LockType.NL;
                }
                return parentType;
            }
            parent = parent.parentContext();
        }
        //if (parent == null) {
        return LockType.NL;
        //}

    }
    public LockType acquireHelper(TransactionContext transaction) { // come back to work on this later
        if (transaction == null) {
            return LockType.NL;
        }
        LockType thisLockType = lockman.getLockType(transaction, getResourceName());
        if (!thisLockType.equals(LockType.NL)) {
            return thisLockType;
        }
        LockContext parentLockType = parentContext();
        while (parentLockType != null) {
            if (!parentLockType.getExplicitLockType(transaction).equals(LockType.NL)) {
                return parentLockType.getExplicitLockType(transaction);
            }
            parentLockType = parentLockType.parentContext();
        }
        return LockType.NL;
    }
    /**
     * Helper method to see if the transaction holds a SIX lock at an ancestor
     * of this context
     * @param transaction the transaction
     * @return true if holds a SIX at an ancestor, false if not
     */
    private boolean hasSIXAncestor(TransactionContext transaction) {
        // TODO(proj4_part2): implement
        LockContext ancestor = parentContext();
        while (ancestor != null) {
            LockType parType = lockman.getLockType(transaction, getResourceName());
            if (parType.equals(LockType.SIX)) {
                return true;
            } else {
                ancestor = ancestor.parentContext();
            }
        }
        return false;
    }

    /**
     * Disables locking descendants. This causes all new child contexts of this
     * context to be readonly. This is used for indices and temporary tables
     * (where we disallow finer-grain locks), the former due to complexity
     * locking B+ trees, and the latter due to the fact that temporary tables
     * are only accessible to one transaction, so finer-grain locks make no
     * sense.
     */
    public void disableChildLocks() {
        this.childLocksDisabled = true;
    }

    /**
     * Gets the parent context.
     */
    public LockContext parentContext() {
        return parent;
    }

    /**
     * Gets the context for the child with name `name` and readable name
     * `readable`
     */
    public synchronized LockContext childContext(String name) {
        LockContext temp = new LockContext(lockman, this, name,
                this.childLocksDisabled || this.readonly);
        LockContext child = this.children.putIfAbsent(name, temp);
        if (child == null) child = temp;
        return child;
    }

    /**
     * Gets the context for the child with name `name`.
     */
    public synchronized LockContext childContext(long name) {
        return childContext(Long.toString(name));
    }

    /**
     * Gets the number of locks held on children a single transaction.
     */
    public int getNumChildren(TransactionContext transaction) {
        return numChildLocks.getOrDefault(transaction.getTransNum(), 0);
    }

    @Override
    public String toString() {
        return "LockContext(" + name.toString() + ")";
    }
}

