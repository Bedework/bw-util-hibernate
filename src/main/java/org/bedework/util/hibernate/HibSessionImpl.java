/* ********************************************************************
    Licensed to Jasig under one or more contributor license
    agreements. See the NOTICE file distributed with this work
    for additional information regarding copyright ownership.
    Jasig licenses this file to you under the Apache License,
    Version 2.0 (the "License"); you may not use this file
    except in compliance with the License. You may obtain a
    copy of the License at:

    http://www.apache.org/licenses/LICENSE-2.0

    Unless required by applicable law or agreed to in writing,
    software distributed under the License is distributed on
    an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
    KIND, either express or implied. See the License for the
    specific language governing permissions and limitations
    under the License.
*/
package org.bedework.util.hibernate;

import org.bedework.util.logging.BwLogger;
import org.bedework.util.logging.Logged;

import org.hibernate.Criteria;
import org.hibernate.FlushMode;
import org.hibernate.LockMode;
import org.hibernate.Query;
import org.hibernate.ReplicationMode;
import org.hibernate.SQLQuery;
import org.hibernate.Session;
import org.hibernate.SessionFactory;
import org.hibernate.StaleStateException;
import org.hibernate.Transaction;

import java.io.Serializable;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Date;
import java.util.List;

/** Convenience class to do the actual hibernate interaction. Intended for
 * one use only.
 *
 * @author Mike Douglass douglm@rpi.edu
 */
public class HibSessionImpl implements Logged, HibSession {
  Session sess;
  transient Transaction tx;
  boolean rolledBack;

  transient Query q;
  transient Criteria crit;

  /** Exception from this session. */
  Throwable exc;

  private final SimpleDateFormat dateFormatter = 
          new SimpleDateFormat("yyyy-MM-dd");

  @Override
  public void init(final SessionFactory sessFactory) throws HibException {
    try {
      sess = sessFactory.openSession();
      rolledBack = false;
      //sess.setFlushMode(FlushMode.COMMIT);
//      tx = sess.beginTransaction();
    } catch (final Throwable t) {
      exc = t;
      tx = null;  // not even started. Should be null anyway
      close();
    }
  }

  @Override
  public Session getSession() {
    return sess;
  }

  @Override
  public boolean isOpen() throws HibException {
    try {
      if (sess == null) {
        return false;
      }
      return sess.isOpen();
    } catch (final Throwable t) {
      handleException(t);
      return false;
    }
  }

  @Override
  public void clear() throws HibException {
    if (exc != null) {
      // Didn't hear me last time?
      throw new HibException(exc);
    }

    try {
      sess.clear();
      tx =  null;
    } catch (final Throwable t) {
      handleException(t);
    }
  }

  @Override
  public void disconnect() throws HibException {
    if (exc != null) {
      // Didn't hear me last time?
      if (exc instanceof HibException) {
        throw (HibException)exc;
      }
      throw new HibException(exc);
    }

    try {
      sess.disconnect();
    } catch (final Throwable t) {
      handleException(t);
    }
  }

  @Override
  public void setFlushMode(final FlushMode val) throws HibException {
    if (exc != null) {
      // Didn't hear me last time?
      throw new HibException(exc);
    }

    try {
      if (tx != null) {
        throw new HibException("Transaction already started");
      }

      sess.setFlushMode(val);
    } catch (final Throwable t) {
      exc = t;
      throw new HibException(t);
    }
  }

  @Override
  public void beginTransaction() throws HibException {
    if (exc != null) {
      // Didn't hear me last time?
      throw new HibException(exc);
    }

    try {
      if (tx != null) {
        throw new HibException("Transaction already started");
      }

      tx = sess.beginTransaction();
      rolledBack = false;
      if (tx == null) {
        throw new HibException("Transaction not started");
      }
    } catch (final HibException cfe) {
      exc = cfe;
      throw cfe;
    } catch (final Throwable t) {
      exc = t;
      throw new HibException(t);
    }
  }

  @Override
  public boolean transactionStarted() {
    return tx != null;
  }

  @Override
  public void commit() throws HibException {
    if (exc != null) {
      // Didn't hear me last time?
      throw new HibException(exc);
    }

    try {
//      if (tx != null &&
//          !tx.wasCommitted() &&
//          !tx.wasRolledBack()) {
        //if (getLogger().isDebugEnabled()) {
        //  getLogger().debug("About to comnmit");
        //}
      if (tx != null) {
        tx.commit();
      }

      tx = null;
    } catch (final Throwable t) {
      exc = t;

      if (t instanceof StaleStateException) {
        throw new DbStaleStateException(t.getMessage());
      }
      throw new HibException(t);
    }
  }

  @Override
  public void rollback() throws HibException {
/*    if (exc != null) {
      // Didn't hear me last time?
      throw new HibException(exc);
    }
*/
    if (getLogger().isDebugEnabled()) {
      getLogger().debug("Enter rollback");
    }
    try {
      if ((tx != null) &&
          !rolledBack) {
        if (getLogger().isDebugEnabled()) {
          getLogger().debug("About to rollback");
        }
        tx.rollback();
        //tx = null;
        clear();
      }
    } catch (final Throwable t) {
      exc = t;
      throw new HibException(t);
    } finally {
      rolledBack = true;
    }
  }

  @Override
  public boolean rolledback() {
    return rolledBack;
  }

  @Override
  public Criteria createCriteria(final Class cl) throws HibException {
    if (exc != null) {
      // Didn't hear me last time?
      throw new HibException(exc);
    }

    try {
      crit = sess.createCriteria(cl);
      q = null;

      return crit;
    } catch (final Throwable t) {
      handleException(t);
      return null;  // Don't get here
    }
  }

  @Override
  public void evict(final Object val) throws HibException {
    if (exc != null) {
      // Didn't hear me last time?
      throw new HibException(exc);
    }

    try {
      sess.evict(val);
    } catch (final Throwable t) {
      handleException(t);
    }
  }

  @Override
  public void createQuery(final String s) throws HibException {
    if (exc != null) {
      // Didn't hear me last time?
      throw new HibException(exc);
    }

    try {
      q = sess.createQuery(s);
      crit = null;
    } catch (final Throwable t) {
      handleException(t);
    }
  }

  @Override
  public void createNoFlushQuery(final String s) throws HibException {
    if (exc != null) {
      // Didn't hear me last time?
      throw new HibException(exc);
    }

    try {
      q = sess.createQuery(s);
      crit = null;
      q.setFlushMode(FlushMode.COMMIT);
    } catch (final Throwable t) {
      handleException(t);
    }
  }

  @Override
  public String getQueryString() throws HibException {
    if (q == null) {
      return "*** no query ***";
    }

    try {
      return q.getQueryString();
    } catch (final Throwable t) {
      handleException(t);
      return null;
    }
  }

  @Override
  public void createSQLQuery(final String s,
                             final String returnAlias,
                             final Class returnClass)
        throws HibException {
    if (exc != null) {
      // Didn't hear me last time?
      throw new HibException(exc);
    }

    try {
      SQLQuery sq = sess.createSQLQuery(s);
      sq.addEntity(returnAlias, returnClass);

      q = sq;
      crit = null;
    } catch (final Throwable t) {
      handleException(t);
    }
  }

  @Override
  public void namedQuery(final String name) throws HibException {
    if (exc != null) {
      // Didn't hear me last time?
      throw new HibException(exc);
    }

    try {
      q = sess.getNamedQuery(name);
      crit = null;
    } catch (final Throwable t) {
      handleException(t);
    }
  }

  /** Mark the query as cacheable
   *
   * @throws HibException on fatal error
   */
  @Override
  public void cacheableQuery() throws HibException {
    if (exc != null) {
      // Didn't hear me last time?
      throw new HibException(exc);
    }

    try {
      q.setCacheable(true);
    } catch (final Throwable t) {
      handleException(t);
    }
  }

  @Override
  public void setString(final String parName, final String parVal) throws HibException {
    if (exc != null) {
      // Didn't hear me last time?
      throw new HibException(exc);
    }

    try {
      q.setString(parName, parVal);
    } catch (final Throwable t) {
      handleException(t);
    }
  }

  @Override
  public void setDate(final String parName, final Date parVal) throws HibException {
    if (exc != null) {
      // Didn't hear me last time?
      throw new HibException(exc);
    }

    try {
      // Remove any time component
      synchronized (dateFormatter) {
        q.setDate(parName, java.sql.Date.valueOf(dateFormatter.format(parVal)));
      }
    } catch (final Throwable t) {
      handleException(t);
    }
  }

  @Override
  public void setBool(final String parName, final boolean parVal) throws HibException {
    if (exc != null) {
      // Didn't hear me last time?
      throw new HibException(exc);
    }

    try {
      q.setBoolean(parName, parVal);
    } catch (final Throwable t) {
      handleException(t);
    }
  }

  @Override
  public void setInt(final String parName, final int parVal) throws HibException {
    if (exc != null) {
      // Didn't hear me last time?
      throw new HibException(exc);
    }

    try {
      q.setInteger(parName, parVal);
    } catch (final Throwable t) {
      handleException(t);
    }
  }

  @Override
  public void setLong(final String parName, final long parVal) throws HibException {
    if (exc != null) {
      // Didn't hear me last time?
      throw new HibException(exc);
    }

    try {
      q.setLong(parName, parVal);
    } catch (final Throwable t) {
      handleException(t);
    }
  }

  @Override
  public void setEntity(final String parName, final Object parVal) throws HibException {
    if (exc != null) {
      // Didn't hear me last time?
      throw new HibException(exc);
    }

    try {
      q.setEntity(parName, parVal);
    } catch (final Throwable t) {
      handleException(t);
    }
  }

  @Override
  public void setParameter(final String parName, final Object parVal) throws HibException {
    if (exc != null) {
      // Didn't hear me last time?
      throw new HibException(exc);
    }

    try {
      q.setParameter(parName, parVal);
    } catch (final Throwable t) {
      handleException(t);
    }
  }

  @Override
  public void setParameterList(final String parName,
                               final Collection parVal) throws HibException {
    if (exc != null) {
      // Didn't hear me last time?
      throw new HibException(exc);
    }

    try {
      q.setParameterList(parName, parVal);
    } catch (final Throwable t) {
      handleException(t);
    }
  }

  @Override
  public void setFirstResult(final int val) throws HibException {
    if (exc != null) {
      // Didn't hear me last time?
      throw new HibException(exc);
    }

    try {
      q.setFirstResult(val);
    } catch (final Throwable t) {
      handleException(t);
    }
  }

  @Override
  public void setMaxResults(final int val) throws HibException {
    if (exc != null) {
      // Didn't hear me last time?
      throw new HibException(exc);
    }

    try {
      q.setMaxResults(val);
    } catch (final Throwable t) {
      handleException(t);
    }
  }

  @Override
  public Object getUnique() throws HibException {
    if (exc != null) {
      // Didn't hear me last time?
      throw new HibException(exc);
    }

    try {
      if (q != null) {
        return q.uniqueResult();
      }

      return crit.uniqueResult();
    } catch (final Throwable t) {
      handleException(t);
      return null;  // Don't get here
    }
  }

  @Override
  public List getList() throws HibException {
    if (exc != null) {
      // Didn't hear me last time?
      throw new HibException(exc);
    }

    try {
      final List<?> l;
      if (q != null) {
        l = q.list();
      } else {
        l = crit.list();
      }

      if (l == null) {
        return new ArrayList();
      }

      return l;
    } catch (final Throwable t) {
      handleException(t);
      return null;  // Don't get here
    }
  }

  @Override
  public int executeUpdate() throws HibException {
    if (exc != null) {
      // Didn't hear me last time?
      throw new HibException(exc);
    }

    try {
      if (q == null) {
        throw new HibException("No query for execute update");
      }

      return q.executeUpdate();
    } catch (final Throwable t) {
      handleException(t);
      return 0;  // Don't get here
    }
  }

  @Override
  public void update(final Object obj) throws HibException {
    if (exc != null) {
      // Didn't hear me last time?
      throw new HibException(exc);
    }

    try {
      beforeSave(obj);
      sess.update(obj);
      deleteSubs(obj);
    } catch (final Throwable t) {
      handleException(t);
    }
  }

  @Override
  public Object merge(Object obj) throws HibException {
    if (exc != null) {
      // Didn't hear me last time?
      throw new HibException(exc);
    }

    try {
      beforeSave(obj);

      obj = sess.merge(obj);
      deleteSubs(obj);

      return obj;
    } catch (final Throwable t) {
      handleException(t, obj);
      return null;
    }
  }

  @Override
  public void saveOrUpdate(final Object obj) throws HibException {
    if (exc != null) {
      // Didn't hear me last time?
      throw new HibException(exc);
    }

    try {
      beforeSave(obj);

      sess.saveOrUpdate(obj);
      deleteSubs(obj);
    } catch (final Throwable t) {
      handleException(t);
    }
  }

  @Override
  public Object saveOrUpdateCopy(final Object obj) throws HibException {
    if (exc != null) {
      // Didn't hear me last time?
      throw new HibException(exc);
    }

    try {
      return sess.merge(obj);
    } catch (final Throwable t) {
      handleException(t);
      return null;  // Don't get here
    }
  }

  @Override
  public Object get(final Class cl,
                    final Serializable id) throws HibException {
    if (exc != null) {
      // Didn't hear me last time?
      throw new HibException(exc);
    }

    try {
      return sess.get(cl, id);
    } catch (final Throwable t) {
      handleException(t);
      return null;  // Don't get here
    }
  }

  @Override
  public Object get(final Class cl, final int id) throws HibException {
    return get(cl, new Integer(id));
  }

  @Override
  public void save(final Object obj) throws HibException {
    if (exc != null) {
      // Didn't hear me last time?
      throw new HibException(exc);
    }

    try {
      beforeSave(obj);
      sess.save(obj);
      deleteSubs(obj);
    } catch (final Throwable t) {
      handleException(t);
    }
  }

  @Override
  public void delete(final Object obj) throws HibException {
    if (exc != null) {
      // Didn't hear me last time?
      throw new HibException(exc);
    }

    try {
      beforeDelete(obj);

      sess.delete(obj);
      deleteSubs(obj);
    } catch (final Throwable t) {
      handleException(t);
    }
  }

  @Override
  public void restore(final Object obj) throws HibException {
    if (exc != null) {
      // Didn't hear me last time?
      throw new HibException(exc);
    }

    try {
      sess.replicate(obj, ReplicationMode.IGNORE);
    } catch (final Throwable t) {
      handleException(t);
    }
  }

  @Override
  public void reAttach(final UnversionedDbentity<?, ?> val) throws HibException {
    if (exc != null) {
      // Didn't hear me last time?
      throw new HibException(exc);
    }

    try {
      if (!val.unsaved()) {
        sess.lock(val, LockMode.NONE);
      }
    } catch (final Throwable t) {
      handleException(t);
    }
  }

  @Override
  public void lockRead(final Object o) throws HibException {
    if (exc != null) {
      // Didn't hear me last time?
      throw new HibException(exc);
    }

    try {
      sess.lock(o, LockMode.READ);
    } catch (final Throwable t) {
      handleException(t);
    }
  }

  @Override
  public void lockUpdate(final Object o) throws HibException {
    if (exc != null) {
      // Didn't hear me last time?
      throw new HibException(exc);
    }

    try {
      sess.lock(o, LockMode.UPGRADE);
    } catch (final Throwable t) {
      handleException(t);
    }
  }

  @Override
  public void flush() throws HibException {
    if (exc != null) {
      // Didn't hear me last time?
      throw new HibException(exc);
    }

    if (getLogger().isDebugEnabled()) {
      getLogger().debug("About to flush");
    }
    try {
      sess.flush();
    } catch (final Throwable t) {
      handleException(t);
    }
  }

  @Override
  public void close() throws HibException {
    if (sess == null) {
      return;
    }

//    throw new HibException("XXXXXXXXXXXXXXXXXXXXXXXXXXXXX");/*
    try {
      if (sess.isDirty()) {
        sess.flush();
      }
      if ((tx != null) && !rolledback()) {
        tx.commit();
      }
    } catch (final Throwable t) {
      if (exc == null) {
        exc = t;
      }
    } finally {
      tx = null;
      if (sess != null) {
        try {
          sess.close();
        } catch (final Throwable t) {}
      }
    }

    sess = null;
    if (exc != null) {
      throw new HibException(exc);
    }
//    */
  }

  private void handleException(final Throwable t) throws HibException {
    handleException(t, null);
  }

  private void handleException(final Throwable t,
                               final Object o) throws HibException {
    try {
      if (debug()) {
        debug("handleException called");
        if (o != null) {
          debug(o.toString());
        }
        error(t);
      }
    } catch (final Throwable ignored) {}

    try {
      if (tx != null) {
        try {
          tx.rollback();
        } catch (final Throwable t1) {
          rollbackException(t1);
        }
        tx = null;
      }
    } finally {
      try {
        sess.close();
      } catch (Throwable ignored) {}
      sess = null;
    }

    exc = t;

    if (t instanceof StaleStateException) {
      throw new DbStaleStateException(t.getMessage());
    }

    throw new HibException(t);
  }

  private void beforeSave(final Object o) throws HibException {
    if (!(o instanceof VersionedDbEntity)) {
      return;
    }

    final VersionedDbEntity<?, ?> ent = (VersionedDbEntity<?, ?>)o;

    ent.beforeSave();
  }

  private void beforeDelete(final Object o) throws HibException {
    if (!(o instanceof VersionedDbEntity)) {
      return;
    }

    final VersionedDbEntity<?, ?> ent = (VersionedDbEntity<?, ?>)o;

    ent.beforeDeletion();
  }

  private void deleteSubs(final Object o) throws HibException {
    if (!(o instanceof VersionedDbEntity)) {
      return;
    }

    final VersionedDbEntity<?, ?> ent = (VersionedDbEntity<?, ?>)o;

    final Collection<VersionedDbEntity<?, ?>> subs =
            ent.getDeletedEntities();
    if (subs == null) {
      return;
    }

    for (final VersionedDbEntity<?, ?> sub: subs) {
      delete(sub);
    }
  }

  /** This is just in case we want to report rollback exceptions. Seems we're
   * likely to get one.
   *
   * @param t   Throwable from the rollback
   */
  private void rollbackException(final Throwable t) {
    error(t);
  }

  /* ====================================================================
   *                   Logged methods
   * ==================================================================== */

  private final BwLogger logger = new BwLogger();

  @Override
  public BwLogger getLogger() {
    if ((logger.getLoggedClass() == null) && (logger.getLoggedName() == null)) {
      logger.setLoggedClass(getClass());
    }

    return logger;
  }
}
